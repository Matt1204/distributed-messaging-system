package com.coen6731.chat.client;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Component;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Font;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.DefaultListModel;
import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JPasswordField;
import javax.swing.JScrollBar;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.ListSelectionModel;
import javax.swing.SwingConstants;
import javax.swing.SwingUtilities;
import javax.swing.WindowConstants;
import javax.swing.border.EmptyBorder;

/**
 * Responsibility: Swing UI for auth + conversation-centric chat interactions.
 * Input: user actions and client callbacks.
 * Output: rendered auth/chat views and send/auth commands.
 */
public class ChatWindow extends JFrame implements ClientUiListener {
  private static final DateTimeFormatter TIME_FMT =
      DateTimeFormatter.ofPattern("HH:mm:ss").withZone(ZoneId.systemDefault());
  private static final String AUTH_CARD = "auth";
  private static final String CHAT_CARD = "chat";

  private static final int BUBBLE_WIDTH = 360;
  private static final int BUBBLE_MIN_HEIGHT = 84;
  private static final int BUBBLE_MAX_HEIGHT = 240;
  private static final int CONTENT_MIN_HEIGHT = 30;
  private static final int CONTENT_MAX_HEIGHT = 170;
  private static final String SEPARATOR_PROMPT = "keep scrolling to fetch older messages";

  private static final int HISTORY_REQUEST_TIMEOUT_SECONDS = 8;

  private final ChatClientSession session;
  private final boolean debugSidebarEnabled;
  private final int historyPageSize;

  private final java.awt.CardLayout cardLayout = new java.awt.CardLayout();
  private final JPanel cardPanel = new JPanel(cardLayout);

  private final JTextField emailField = new JTextField(20);
  private final JPasswordField passwordField = new JPasswordField(20);
  private final JButton loginButton = new JButton("Login");
  private final JButton registerButton = new JButton("Register");
  private final JLabel authStatusLabel = new JLabel("Please login or register.");
  private final JLabel authConnectionLabel = new JLabel("Server: connecting");

  private final DefaultListModel<ConversationItem> conversationModel = new DefaultListModel<>();
  private final JList<ConversationItem> conversationList = new JList<>(conversationModel);
  private final JButton startConversationButton = new JButton("Start New Conversation");

  private final JLabel currentUserEmailLabel = new JLabel("Current User: -");
  private final JLabel chatTitleLabel = new JLabel("Chat");
  private final JLabel loadingLabel = new JLabel(" ", SwingConstants.CENTER);
  private final JPanel messageListPanel = new JPanel();
  private final JScrollPane messageScrollPane = new JScrollPane(messageListPanel);
  private final JTextField toEmailField = new JTextField(24);
  private final JTextField messageField = new JTextField(36);
  private final JButton sendButton = new JButton("Send");

  private final JTextArea debugArea = new JTextArea(18, 36);

  private final Map<String, ConversationViewState> conversationStateById = new ConcurrentHashMap<>();
  private final Map<String, CompletableFuture<Integer>> pendingHistoryFetchByConversation =
      new ConcurrentHashMap<>();

  private final AtomicLong selectionGeneration = new AtomicLong(0L);

  private boolean suppressConversationSelectionHandling = false;

  public ChatWindow(
      ChatClientSession session,
      boolean debugSidebarEnabled,
      int historyPageSize) {
    super("Chat Client");
    this.session = session;
    this.debugSidebarEnabled = debugSidebarEnabled;
    this.historyPageSize = normalizePositive(historyPageSize, 20, 200);
    this.session.setUiListener(this);
    buildUi();
    wireActions();
  }

  private void buildUi() {
    setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);

    JPanel root = new JPanel(new BorderLayout(8, 8));
    cardPanel.add(buildAuthPanel(), AUTH_CARD);
    cardPanel.add(buildChatPanel(), CHAT_CARD);
    cardLayout.show(cardPanel, AUTH_CARD);
    root.add(cardPanel, BorderLayout.CENTER);

    if (debugSidebarEnabled) {
      debugArea.setEditable(false);
      JPanel debugPanel = new JPanel(new BorderLayout());
      debugPanel.add(new JLabel("Debug"), BorderLayout.NORTH);
      debugPanel.add(new JScrollPane(debugArea), BorderLayout.CENTER);
      root.add(debugPanel, BorderLayout.EAST);
    }

    setContentPane(root);
    pack();
    setMinimumSize(new Dimension(1080, 620));
    setLocationRelativeTo(null);
  }

  private JPanel buildAuthPanel() {
    JPanel panel = new JPanel(new BorderLayout(8, 8));
    JPanel form = new JPanel(new FlowLayout(FlowLayout.LEFT));
    form.add(new JLabel("Email:"));
    form.add(emailField);
    form.add(new JLabel("Password:"));
    form.add(passwordField);
    form.add(loginButton);
    form.add(registerButton);

    JPanel status = new JPanel(new BorderLayout());
    status.add(authConnectionLabel, BorderLayout.NORTH);
    status.add(authStatusLabel, BorderLayout.SOUTH);

    panel.add(form, BorderLayout.NORTH);
    panel.add(status, BorderLayout.CENTER);
    return panel;
  }

  private JPanel buildChatPanel() {
    JPanel panel = new JPanel(new BorderLayout(8, 8));
    panel.add(currentUserEmailLabel, BorderLayout.NORTH);

    JPanel leftPane = new JPanel(new BorderLayout(6, 6));
    leftPane.setBorder(BorderFactory.createEmptyBorder(4, 4, 4, 4));
    leftPane.add(startConversationButton, BorderLayout.NORTH);
    conversationList.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
    leftPane.add(new JScrollPane(conversationList), BorderLayout.CENTER);

    JPanel rightPane = new JPanel(new BorderLayout(8, 8));
    JPanel rightTopPanel = new JPanel(new BorderLayout(4, 4));
    rightTopPanel.add(chatTitleLabel, BorderLayout.NORTH);
    loadingLabel.setForeground(new Color(60, 60, 60));
    rightTopPanel.add(loadingLabel, BorderLayout.SOUTH);
    rightPane.add(rightTopPanel, BorderLayout.NORTH);

    messageListPanel.setLayout(new BoxLayout(messageListPanel, BoxLayout.Y_AXIS));
    messageListPanel.setBorder(new EmptyBorder(8, 8, 8, 8));
    messageListPanel.setBackground(new Color(248, 248, 248));
    messageScrollPane.getVerticalScrollBar().setUnitIncrement(24);
    rightPane.add(messageScrollPane, BorderLayout.CENTER);

    JPanel sendPanel = new JPanel(new FlowLayout(FlowLayout.LEFT));
    sendPanel.add(new JLabel("To:"));
    sendPanel.add(toEmailField);
    sendPanel.add(new JLabel("Message:"));
    sendPanel.add(messageField);
    sendPanel.add(sendButton);
    rightPane.add(sendPanel, BorderLayout.SOUTH);

    JSplitPane splitPane = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT, leftPane, rightPane);
    splitPane.setResizeWeight(0.3);
    panel.add(splitPane, BorderLayout.CENTER);
    return panel;
  }

  private void wireActions() {
    loginButton.addActionListener(e -> runAuth(true));
    registerButton.addActionListener(e -> runAuth(false));

    sendButton.addActionListener(e -> runSend());
    messageField.addActionListener(e -> runSend());

    startConversationButton.addActionListener(
        e -> {
          selectionGeneration.incrementAndGet();
          conversationList.clearSelection();
          clearConversationPane("No conversation selected. Start a new conversation.");
          chatTitleLabel.setText("Chat - New conversation");
          toEmailField.setText("");
          toEmailField.setEditable(true);
          toEmailField.requestFocusInWindow();
          setLoading(false, " ");
        });

    conversationList.addListSelectionListener(
        e -> {
          if (!e.getValueIsAdjusting() && !suppressConversationSelectionHandling) {
            loadConversationForCurrentSelection();
          }
        });

    messageScrollPane.addMouseWheelListener(
        e -> {
          if (e.getWheelRotation() >= 0) {
            return;
          }
          JScrollBar verticalBar = messageScrollPane.getVerticalScrollBar();
          if (verticalBar.getValue() == 0 && handleTopScrollAttempt()) {
            verticalBar.setValue(0);
            e.consume();
          }
        });

    addWindowListener(
        new java.awt.event.WindowAdapter() {
          @Override
          public void windowClosed(java.awt.event.WindowEvent e) {
            session.close();
          }
        });
  }

  private boolean handleTopScrollAttempt() {
    ConversationItem selected = conversationList.getSelectedValue();
    if (selected == null) {
      return false;
    }

    ConversationViewState state =
        conversationStateById.computeIfAbsent(selected.conversationId, ignored -> new ConversationViewState());

    if (state.isFetchingHistory) {
      return true;
    }
    if (state.historyExhausted || state.renderedLowSequenceId <= 1) {
      state.historyExhausted = true;
      state.separationBarVisible = true;
      state.separatorGateArmed = false;
      rerenderSelectedConversation(state, ScrollMode.KEEP_POSITION);
      return true;
    }

    state.separationBarVisible = true;
    if (!state.separatorGateArmed) {
      state.separatorGateArmed = true;
      rerenderSelectedConversation(state, ScrollMode.KEEP_POSITION);
      return true;
    }

    state.separatorGateArmed = false;
    loadOlderHistoryForSelectedConversation(selected.conversationId);
    return true;
  }

  private void runAuth(boolean login) {
    String email = emailField.getText().trim();
    char[] passwordChars = passwordField.getPassword();
    String password = new String(passwordChars).trim();
    java.util.Arrays.fill(passwordChars, '\0');

    if (email.isEmpty() || password.isEmpty()) {
      authStatusLabel.setText("Email and password cannot be empty.");
      return;
    }

    authStatusLabel.setText(login ? "Logging in..." : "Registering...");
    CompletableFuture.supplyAsync(() -> login ? session.login(email, password) : session.register(email, password))
        .whenComplete(
            (ok, err) ->
                SwingUtilities.invokeLater(
                    () -> {
                      if (err != null) {
                        authStatusLabel.setText("Auth failed: " + err.getMessage());
                        return;
                      }
                      if (!Boolean.TRUE.equals(ok)) {
                        String authErr = session.getLastAuthError();
                        authStatusLabel.setText(
                            "Auth failed: " + (authErr == null ? "unknown error" : authErr));
                      }
                    }));
  }

  private void runSend() {
    ConversationItem selected = conversationList.getSelectedValue();
    String toEmail = selected != null ? safe(selected.peerEmail) : toEmailField.getText().trim();
    String text = messageField.getText().trim();

    if (toEmail.isEmpty() || text.isEmpty()) {
      JOptionPane.showMessageDialog(
          this,
          "Recipient email and message text are required.",
          "Send Message",
          JOptionPane.WARNING_MESSAGE);
      return;
    }

    String conversationId = selected != null ? selected.conversationId : "";
    String peerUserId = selected != null ? selected.peerUserId : "";

    CompletableFuture.runAsync(() -> session.sendMessage(toEmail, text, conversationId, peerUserId));
    messageField.setText("");
  }

  private void refreshConversationList() {
    List<DatabaseManager.ConversationSummary> conversations = session.listConversations();
    String previousSelection =
        conversationList.getSelectedValue() == null
            ? null
            : conversationList.getSelectedValue().conversationId;

    suppressConversationSelectionHandling = true;
    try {
      conversationModel.clear();
      for (DatabaseManager.ConversationSummary conversation : conversations) {
        conversationModel.addElement(
            new ConversationItem(
                conversation.conversationId(),
                conversation.peerUserId(),
                conversation.peerEmail(),
                conversation.lastMessageAt(),
                conversation.lastMessagePreview()));
      }

      if (previousSelection != null) {
        for (int i = 0; i < conversationModel.size(); i++) {
          if (previousSelection.equals(conversationModel.get(i).conversationId)) {
            conversationList.setSelectedIndex(i);
            return;
          }
        }
      }

      if (!conversationModel.isEmpty() && conversationList.getSelectedIndex() < 0) {
        conversationList.setSelectedIndex(0);
      }
    } finally {
      suppressConversationSelectionHandling = false;
    }
  }

  private void loadConversationForCurrentSelection() {
    ConversationItem selected = conversationList.getSelectedValue();
    long generation = selectionGeneration.incrementAndGet();

    if (selected == null) {
      clearConversationPane("No conversation selected. Start a new conversation.");
      toEmailField.setEditable(true);
      setLoading(false, " ");
      return;
    }

    chatTitleLabel.setText("Chat - " + safe(selected.peerEmail));
    toEmailField.setText(safe(selected.peerEmail));
    toEmailField.setEditable(false);

    ConversationViewState state =
        conversationStateById.computeIfAbsent(selected.conversationId, ignored -> new ConversationViewState());

    state.isFetchingHistory = true;
    state.historyExhausted = false;
    state.separationBarVisible = false;
    state.separatorGateArmed = false;
    state.renderedLowSequenceId = 0L;
    state.renderedHighSequenceId = 0L;
    state.historyExpandedByUser = false;
    setLoading(true, "Loading conversation...");

    CompletableFuture.supplyAsync(() -> loadInitialPage(selected.conversationId))
        .whenComplete(
            (result, throwable) ->
                SwingUtilities.invokeLater(
                    () -> {
                      if (!isSelectedConversation(selected.conversationId, generation)) {
                        return;
                      }
                      state.isFetchingHistory = false;
                      if (throwable != null) {
                        appendDebugLine(
                            "[history] initial load failed conv="
                                + selected.conversationId
                                + " error="
                                + safe(throwable.getMessage()));
                        clearConversationPane("Failed to load messages.");
                        setLoading(false, " ");
                        return;
                      }
                      applyInitialFetchResultToState(state, result);
                      rerenderSelectedConversation(state, ScrollMode.TO_BOTTOM);
                      setLoading(false, " ");
                    }));
  }

  private FetchResult loadInitialPage(String conversationId) {
    long latestKnownSequenceId = Math.max(0L, session.getLatestMessageSequenceId(conversationId));
    long maxStoredSequenceId = Math.max(0L, session.getMaxStoredSequenceId(conversationId));
    long high = Math.max(latestKnownSequenceId, maxStoredSequenceId);
    return fetchSequenceRangeWithBackfill(conversationId, high, historyPageSize);
  }

  private void loadOlderHistoryForSelectedConversation(String conversationId) {
    ConversationViewState state = conversationStateById.get(conversationId);
    if (state == null || state.isFetchingHistory || state.historyExhausted) {
      return;
    }

    long targetHigh = state.renderedLowSequenceId - 1;
    if (targetHigh <= 0) {
      state.historyExhausted = true;
      state.separationBarVisible = true;
      rerenderSelectedConversation(state, ScrollMode.KEEP_POSITION);
      return;
    }

    state.isFetchingHistory = true;
    state.separationBarVisible = true;
    setLoading(true, "Loading older messages...");
    long generation = selectionGeneration.get();

    CompletableFuture.supplyAsync(() -> fetchSequenceRangeWithBackfill(conversationId, targetHigh, historyPageSize))
        .whenComplete(
            (result, throwable) ->
                SwingUtilities.invokeLater(
                    () -> {
                      ConversationViewState liveState = conversationStateById.get(conversationId);
                      if (liveState == null) {
                        return;
                      }
                      liveState.isFetchingHistory = false;

                      if (!isSelectedConversation(conversationId, generation)) {
                        return;
                      }

                      if (throwable != null) {
                        appendDebugLine(
                            "[history] older load failed conv="
                                + conversationId
                                + " error="
                                + safe(throwable.getMessage()));
                        liveState.separationBarVisible = true;
                        liveState.separatorGateArmed = false;
                        rerenderSelectedConversation(liveState, ScrollMode.KEEP_POSITION);
                        setLoading(false, " ");
                        return;
                      }

                      mergeOlderFetchResultIntoState(liveState, result);
                      rerenderSelectedConversation(liveState, ScrollMode.TO_TOP);
                      setLoading(false, " ");
                    }));
  }

  private FetchResult fetchSequenceRangeWithBackfill(String conversationId, long high, int pageSize) {
    if (high <= 0) {
      return new FetchResult(List.of(), 0L, 0L, true, high);
    }

    long low = Math.max(1L, high - pageSize + 1L);
    Set<Long> existing = session.listExistingSequenceIdsInRange(conversationId, low, high);
    List<SequenceSegment> missingSegments = computeMissingSegments(low, high, existing);

    for (SequenceSegment segment : missingSegments) {
      requestHistorySegmentAndAwait(conversationId, segment.highInclusive, segment.size());
    }

    List<DatabaseManager.MessageRow> rows = session.listMessagesBySequenceRange(conversationId, low, high);
    boolean reachedStart = low <= 1;
    boolean exhausted = reachedStart || rows.isEmpty();
    return new FetchResult(rows, low, high, exhausted, high);
  }

  private void requestHistorySegmentAndAwait(String conversationId, long beforeSequenceId, int quantity) {
    if (quantity <= 0) {
      return;
    }

    CompletableFuture<Integer> completion = new CompletableFuture<>();
    CompletableFuture<Integer> existing =
        pendingHistoryFetchByConversation.putIfAbsent(conversationId, completion);
    if (existing != null) {
      throw new CompletionException(
          new IllegalStateException("Concurrent history fetch detected for conversation " + conversationId));
    }

    try {
      session.requestMessageHistory(conversationId, beforeSequenceId, quantity);
      completion.get(HISTORY_REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    } catch (TimeoutException timeoutException) {
      throw new CompletionException(timeoutException);
    } catch (InterruptedException interruptedException) {
      Thread.currentThread().interrupt();
      throw new CompletionException(interruptedException);
    } catch (Exception e) {
      throw new CompletionException(e);
    } finally {
      pendingHistoryFetchByConversation.remove(conversationId, completion);
    }
  }

  private List<SequenceSegment> computeMissingSegments(
      long lowInclusive, long highInclusive, Set<Long> existingSequenceIds) {
    List<SequenceSegment> segments = new ArrayList<>();
    long cursor = highInclusive;

    while (cursor >= lowInclusive) {
      if (existingSequenceIds.contains(cursor)) {
        cursor--;
        continue;
      }

      long segmentHigh = cursor;
      while (cursor >= lowInclusive && !existingSequenceIds.contains(cursor)) {
        cursor--;
      }
      long segmentLow = cursor + 1;
      segments.add(new SequenceSegment(segmentLow, segmentHigh));
    }

    return segments;
  }

  private void applyInitialFetchResultToState(ConversationViewState state, FetchResult result) {
    state.latestKnownSequenceId = Math.max(state.latestKnownSequenceId, result.effectiveHigh);

    if (result.rows.isEmpty()) {
      state.renderedLowSequenceId = 0L;
      state.renderedHighSequenceId = 0L;
      state.historyExhausted = true;
      state.separationBarVisible = true;
      state.separatorGateArmed = false;
      return;
    }

    long firstSequence = result.rows.get(0).sequenceId();
    long lastSequence = result.rows.get(result.rows.size() - 1).sequenceId();
    long loadedLow = Math.min(firstSequence, lastSequence);
    long loadedHigh = Math.max(firstSequence, lastSequence);

    state.renderedLowSequenceId = loadedLow;
    state.renderedHighSequenceId = loadedHigh;

    state.historyExhausted = result.historyExhausted || state.renderedLowSequenceId <= 1;
    state.separationBarVisible = true;
    state.separatorGateArmed = false;
  }

  private void mergeOlderFetchResultIntoState(ConversationViewState state, FetchResult result) {
    state.latestKnownSequenceId = Math.max(state.latestKnownSequenceId, result.effectiveHigh);

    if (result.rows.isEmpty()) {
      state.historyExhausted = true;
      state.separationBarVisible = true;
      state.separatorGateArmed = false;
      return;
    }

    long firstSequence = result.rows.get(0).sequenceId();
    long lastSequence = result.rows.get(result.rows.size() - 1).sequenceId();
    long loadedLow = Math.min(firstSequence, lastSequence);
    long loadedHigh = Math.max(firstSequence, lastSequence);

    if (state.renderedLowSequenceId <= 0 || state.renderedHighSequenceId < state.renderedLowSequenceId) {
      state.renderedLowSequenceId = loadedLow;
      state.renderedHighSequenceId = loadedHigh;
    } else {
      if (loadedLow < state.renderedLowSequenceId) {
        state.historyExpandedByUser = true;
      }
      state.renderedLowSequenceId = Math.min(state.renderedLowSequenceId, loadedLow);
      state.renderedHighSequenceId = Math.max(state.renderedHighSequenceId, loadedHigh);
    }

    state.historyExhausted = result.historyExhausted || state.renderedLowSequenceId <= 1;
    state.separationBarVisible = true;
    state.separatorGateArmed = false;
  }

  private void rerenderSelectedConversation(ConversationViewState state, ScrollMode scrollMode) {
    ConversationItem selected = conversationList.getSelectedValue();
    if (selected == null) {
      return;
    }

    String conversationId = selected.conversationId;
    chatTitleLabel.setText(
        "Chat - "
            + safe(selected.peerEmail)
            + " [latest_known_sequence_id="
            + state.latestKnownSequenceId
            + ", rendered="
            + state.renderedLowSequenceId
            + ".."
            + state.renderedHighSequenceId
            + "]");

    List<DatabaseManager.MessageRow> rows;
    if (state.renderedLowSequenceId > 0 && state.renderedHighSequenceId >= state.renderedLowSequenceId) {
      rows =
          session.listMessagesBySequenceRange(
              conversationId, state.renderedLowSequenceId, state.renderedHighSequenceId);
    } else {
      rows = List.of();
    }

    renderMessages(rows, state, scrollMode);
  }

  private void renderMessages(
      List<DatabaseManager.MessageRow> rows, ConversationViewState state, ScrollMode scrollMode) {
    JScrollBar bar = messageScrollPane.getVerticalScrollBar();
    final int beforeMax = bar.getMaximum();
    final int beforeValue = bar.getValue();

    messageListPanel.removeAll();

    if (state.separationBarVisible) {
      messageListPanel.add(createSeparationBar(state));
      messageListPanel.add(Box.createVerticalStrut(8));
    }

    if (rows.isEmpty()) {
      if (state.historyExhausted) {
        messageListPanel.add(createPlaceholderLabel("No older messages."));
      } else {
        messageListPanel.add(createPlaceholderLabel("No local messages."));
      }
    } else {
      for (DatabaseManager.MessageRow row : rows) {
        messageListPanel.add(createBubbleRow(row));
        messageListPanel.add(Box.createVerticalStrut(6));
      }
    }

    messageListPanel.revalidate();
    messageListPanel.repaint();

    SwingUtilities.invokeLater(
        () -> {
          if (scrollMode == ScrollMode.TO_BOTTOM) {
            bar.setValue(bar.getMaximum());
            return;
          }
          if (scrollMode == ScrollMode.TO_TOP) {
            bar.setValue(0);
            return;
          }
          if (scrollMode == ScrollMode.PRESERVE_VIEWPORT) {
            int delta = bar.getMaximum() - beforeMax;
            bar.setValue(Math.max(0, beforeValue + delta));
          }
        });
  }

  private JLabel createSeparationBar(ConversationViewState state) {
    String text;
    if (state.historyExhausted) {
      text = "No older messages";
    } else if (state.isFetchingHistory) {
      text = "Loading older messages...";
    } else {
      text = SEPARATOR_PROMPT;
    }

    JLabel label = new JLabel(text, SwingConstants.CENTER);
    label.setOpaque(true);
    label.setBackground(new Color(231, 236, 245));
    label.setForeground(new Color(67, 76, 101));
    label.setBorder(BorderFactory.createCompoundBorder(
        BorderFactory.createLineBorder(new Color(202, 210, 229)),
        BorderFactory.createEmptyBorder(6, 10, 6, 10)));
    label.setAlignmentX(Component.CENTER_ALIGNMENT);
    label.setMaximumSize(new Dimension(Integer.MAX_VALUE, 32));
    return label;
  }

  private JLabel createPlaceholderLabel(String text) {
    JLabel placeholder = new JLabel(text);
    placeholder.setForeground(new Color(100, 100, 100));
    placeholder.setBorder(new EmptyBorder(12, 8, 12, 8));
    return placeholder;
  }

  private JPanel createBubbleRow(DatabaseManager.MessageRow row) {
    boolean outbound = "OUTBOUND".equalsIgnoreCase(safe(row.direction()));

    JPanel rowPanel = new JPanel(new BorderLayout());
    rowPanel.setOpaque(false);

    JPanel bubble = new JPanel(new BorderLayout(4, 4));
    bubble.setBorder(BorderFactory.createEmptyBorder(6, 10, 6, 10));
    bubble.setBackground(outbound ? new Color(196, 239, 186) : new Color(236, 236, 236));
    bubble.setOpaque(true);

    String sender = outbound ? "You" : safe(row.senderEmail());
    JLabel meta =
        new JLabel(
            sender + "  " + TIME_FMT.format(Instant.ofEpochMilli(Math.max(0L, row.sentAtMs()))));
    meta.setFont(meta.getFont().deriveFont(Font.PLAIN, 11f));
    meta.setForeground(new Color(70, 70, 70));

    JTextArea content = new JTextArea(safe(row.content()));
    content.setWrapStyleWord(true);
    content.setLineWrap(true);
    content.setEditable(false);
    content.setOpaque(false);
    content.setBorder(null);
    content.setFont(content.getFont().deriveFont(Font.PLAIN, 13f));
    content.setAlignmentX(Component.LEFT_ALIGNMENT);

    int textWidth = BUBBLE_WIDTH - 24;
    content.setSize(new Dimension(textWidth, Integer.MAX_VALUE));
    int preferredContentHeight = content.getPreferredSize().height;
    int boundedContentHeight = clamp(preferredContentHeight, CONTENT_MIN_HEIGHT, CONTENT_MAX_HEIGHT);
    content.setPreferredSize(new Dimension(textWidth, boundedContentHeight));

    JLabel sequenceLabel = new JLabel("sequenceId=" + row.sequenceId());
    sequenceLabel.setFont(sequenceLabel.getFont().deriveFont(Font.PLAIN, 11f));
    sequenceLabel.setForeground(new Color(80, 80, 80));

    bubble.add(meta, BorderLayout.NORTH);
    bubble.add(content, BorderLayout.CENTER);
    bubble.add(sequenceLabel, BorderLayout.SOUTH);

    int bubbleHeight = clamp(36 + boundedContentHeight + 26, BUBBLE_MIN_HEIGHT, BUBBLE_MAX_HEIGHT);
    Dimension bubbleSize = new Dimension(BUBBLE_WIDTH, bubbleHeight);
    bubble.setPreferredSize(bubbleSize);
    bubble.setMinimumSize(new Dimension(BUBBLE_WIDTH, BUBBLE_MIN_HEIGHT));
    bubble.setMaximumSize(new Dimension(BUBBLE_WIDTH, BUBBLE_MAX_HEIGHT));

    if (outbound) {
      rowPanel.add(bubble, BorderLayout.EAST);
    } else {
      rowPanel.add(bubble, BorderLayout.WEST);
    }
    return rowPanel;
  }

  private void clearConversationPane(String message) {
    messageListPanel.removeAll();
    messageListPanel.add(createPlaceholderLabel(message));
    messageListPanel.revalidate();
    messageListPanel.repaint();
  }

  private void setLoading(boolean loading, String text) {
    loadingLabel.setText(loading ? text : " ");
  }

  private void appendDebugLine(String line) {
    if (!debugSidebarEnabled) {
      return;
    }
    debugArea.append(line + System.lineSeparator());
    debugArea.setCaretPosition(debugArea.getDocument().getLength());
  }

  @Override
  public void onInfo(String text) {
    SwingUtilities.invokeLater(() -> appendDebugLine(text));
  }

  @Override
  public void onConnectionState(boolean connected) {
    SwingUtilities.invokeLater(
        () -> {
          authConnectionLabel.setText("Server: " + (connected ? "connected" : "reconnecting"));
          appendDebugLine("[client] connection state: " + (connected ? "connected" : "reconnecting"));
        });
  }

  @Override
  public void onAuthState(boolean authenticated, String email, String error) {
    SwingUtilities.invokeLater(
        () -> {
          if (authenticated) {
            authStatusLabel.setText("Authenticated.");
            currentUserEmailLabel.setText("Current User: " + safe(email));
            chatTitleLabel.setText("Chat - " + email);
            cardLayout.show(cardPanel, CHAT_CARD);
            refreshConversationList();
            loadConversationForCurrentSelection();
          } else {
            currentUserEmailLabel.setText("Current User: -");
            cardLayout.show(cardPanel, AUTH_CARD);
            if (error != null && !error.isBlank()) {
              authStatusLabel.setText("Auth failed: " + error);
            }
          }
        });
  }

  @Override
  public void onChatMessage(String conversationId, String fromEmail, String text, String msgId, long sentAtMs) {
    SwingUtilities.invokeLater(
        () -> {
          appendDebugLine(
              "[inbound] "
                  + fromEmail
                  + " -> conv="
                  + conversationId
                  + " msgId="
                  + msgId
                  + " ts="
                  + sentAtMs);
          refreshConversationList();

          ConversationItem selected = conversationList.getSelectedValue();
          if (selected != null && conversationId.equals(selected.conversationId)) {
            ConversationViewState state = conversationStateById.get(conversationId);
            if (state != null && !state.isFetchingHistory) {
              boolean autoScroll = isViewportNearBottom();
              state.latestKnownSequenceId =
                  Math.max(state.latestKnownSequenceId, session.getLatestMessageSequenceId(conversationId));
              long latestStored = session.getMaxStoredSequenceId(conversationId);
              if (latestStored > 0) {
                if (state.renderedLowSequenceId <= 0
                    || state.renderedHighSequenceId < state.renderedLowSequenceId) {
                  state.renderedLowSequenceId = Math.max(1L, latestStored - historyPageSize + 1L);
                  state.historyExpandedByUser = false;
                }
                if (!state.historyExpandedByUser) {
                  state.renderedHighSequenceId = latestStored;
                  state.renderedLowSequenceId = Math.max(1L, latestStored - historyPageSize + 1L);
                } else {
                  state.renderedHighSequenceId = Math.max(state.renderedHighSequenceId, latestStored);
                }
              }
              rerenderSelectedConversation(
                  state, autoScroll ? ScrollMode.TO_BOTTOM : ScrollMode.KEEP_POSITION);
            }
          }
        });
  }

  @Override
  public void onSendAck(String clientMsgId, boolean success, String code, String reason) {
    SwingUtilities.invokeLater(
        () -> {
          if (!success) {
            JOptionPane.showMessageDialog(
                this,
                "Send failed: [" + safe(code) + "] " + safe(reason),
                "Send Failed",
                JOptionPane.ERROR_MESSAGE);
          }
          appendDebugLine(
              "[ack] clientMsgId="
                  + clientMsgId
                  + " success="
                  + success
                  + " code="
                  + safe(code)
                  + " reason="
                  + safe(reason));
        });
  }

  @Override
  public void onConversationDataChanged() {
    SwingUtilities.invokeLater(
        () -> {
          refreshConversationList();
          ConversationItem selected = conversationList.getSelectedValue();
          if (selected == null) {
            return;
          }
          ConversationViewState state = conversationStateById.get(selected.conversationId);
          if (state == null) {
            // New selection (or newly created conversation after send-ack): bootstrap page state.
            loadConversationForCurrentSelection();
            return;
          }
          if (state.isFetchingHistory) {
            return;
          }

          long latestKnown = session.getLatestMessageSequenceId(selected.conversationId);
          long latestStored = session.getMaxStoredSequenceId(selected.conversationId);
          long latestAvailable = Math.max(latestKnown, latestStored);
          if (latestAvailable > state.latestKnownSequenceId) {
            state.latestKnownSequenceId = latestAvailable;
          }
          if (latestStored > state.renderedHighSequenceId) {
            boolean autoScroll = isViewportNearBottom();
            if (state.renderedLowSequenceId <= 0
                || state.renderedHighSequenceId < state.renderedLowSequenceId) {
              state.renderedLowSequenceId = Math.max(1L, latestStored - historyPageSize + 1L);
              state.historyExpandedByUser = false;
            }
            if (!state.historyExpandedByUser) {
              state.renderedHighSequenceId = latestStored;
              state.renderedLowSequenceId = Math.max(1L, latestStored - historyPageSize + 1L);
            } else {
              state.renderedHighSequenceId = latestStored;
            }
            rerenderSelectedConversation(
                state, autoScroll ? ScrollMode.TO_BOTTOM : ScrollMode.KEEP_POSITION);
            return;
          }

          if (state.renderedLowSequenceId > 0
              && state.renderedHighSequenceId >= state.renderedLowSequenceId) {
            rerenderSelectedConversation(state, ScrollMode.KEEP_POSITION);
          }
        });
  }

  @Override
  public void onHistoryResultSummary(String conversationId, long startSequenceId, int messageCount) {
    SwingUtilities.invokeLater(
        () -> {
          appendDebugLine(
              "[history] conv="
                  + conversationId
                  + " startSequenceId="
                  + startSequenceId
                  + " count="
                  + messageCount);
          CompletableFuture<Integer> pending = pendingHistoryFetchByConversation.remove(conversationId);
          if (pending != null) {
            pending.complete(messageCount);
          }
        });
  }

  @Override
  public void onCatchupResultSummary(String conversationId, long startSequenceId, int messageCount) {
    SwingUtilities.invokeLater(
        () ->
            appendDebugLine(
                "[catchup] conv="
                    + conversationId
                    + " startSequenceId="
                    + startSequenceId
                    + " count="
                    + messageCount));
  }

  @Override
  public void onError(String code, String reason) {
    SwingUtilities.invokeLater(
        () -> {
          appendDebugLine("ERROR code=" + code + " reason=" + reason);
          for (CompletableFuture<Integer> pending : pendingHistoryFetchByConversation.values()) {
            pending.completeExceptionally(new IllegalStateException("Server error: " + safe(code) + " " + safe(reason)));
          }
          pendingHistoryFetchByConversation.clear();
        });
  }

  private boolean isSelectedConversation(String conversationId, long generation) {
    ConversationItem selected = conversationList.getSelectedValue();
    return selected != null
        && conversationId.equals(selected.conversationId)
        && selectionGeneration.get() == generation;
  }

  private String safe(String value) {
    return value == null ? "" : value;
  }

  private int normalizePositive(int candidate, int fallback, int maxValue) {
    if (candidate <= 0) {
      return fallback;
    }
    return Math.min(candidate, maxValue);
  }

  private int clamp(int value, int minValue, int maxValue) {
    if (value < minValue) {
      return minValue;
    }
    return Math.min(value, maxValue);
  }

  private boolean isViewportNearBottom() {
    JScrollBar bar = messageScrollPane.getVerticalScrollBar();
    int slackPx = 24;
    int viewportBottom = bar.getValue() + bar.getVisibleAmount();
    return viewportBottom >= (bar.getMaximum() - slackPx);
  }

  private enum ScrollMode {
    TO_BOTTOM,
    TO_TOP,
    PRESERVE_VIEWPORT,
    KEEP_POSITION
  }

  private static final class FetchResult {
    private final List<DatabaseManager.MessageRow> rows;
    private final long low;
    private final long high;
    private final boolean historyExhausted;
    private final long effectiveHigh;

    private FetchResult(
        List<DatabaseManager.MessageRow> rows,
        long low,
        long high,
        boolean historyExhausted,
        long effectiveHigh) {
      this.rows = rows;
      this.low = low;
      this.high = high;
      this.historyExhausted = historyExhausted;
      this.effectiveHigh = effectiveHigh;
    }
  }

  private static final class SequenceSegment {
    private final long lowInclusive;
    private final long highInclusive;

    private SequenceSegment(long lowInclusive, long highInclusive) {
      this.lowInclusive = lowInclusive;
      this.highInclusive = highInclusive;
    }

    private int size() {
      return (int) (highInclusive - lowInclusive + 1L);
    }
  }

  private static final class ConversationViewState {
    private long latestKnownSequenceId;
    private long renderedHighSequenceId;
    private long renderedLowSequenceId;
    private boolean historyExpandedByUser;
    private boolean historyExhausted;
    private boolean isFetchingHistory;
    private boolean separatorGateArmed;
    private boolean separationBarVisible;
  }

  private static final class ConversationItem {
    private final String conversationId;
    private final String peerUserId;
    private final String peerEmail;
    private final long lastMessageAt;
    private final String preview;

    private ConversationItem(
        String conversationId,
        String peerUserId,
        String peerEmail,
        long lastMessageAt,
        String preview) {
      this.conversationId = conversationId;
      this.peerUserId = peerUserId;
      this.peerEmail = peerEmail;
      this.lastMessageAt = lastMessageAt;
      this.preview = preview;
    }

    @Override
    public String toString() {
      String displayEmail = peerEmail == null || peerEmail.isBlank() ? peerUserId : peerEmail;
      String snippet = preview == null ? "" : preview;
      String time =
          lastMessageAt > 0 ? TIME_FMT.format(Instant.ofEpochMilli(lastMessageAt)) : "--:--:--";
      return displayEmail + "  [" + time + "]  " + snippet;
    }
  }
}
