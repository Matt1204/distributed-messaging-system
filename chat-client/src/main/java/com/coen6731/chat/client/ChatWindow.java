package com.coen6731.chat.client;

import java.awt.BorderLayout;
import java.awt.CardLayout;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import javax.swing.BorderFactory;
import javax.swing.DefaultListModel;
import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JPasswordField;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.ListSelectionModel;
import javax.swing.SwingUtilities;
import javax.swing.WindowConstants;

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

  private final ChatClientSession session;
  private final boolean debugSidebarEnabled;
  // TEST ONLY - TO BE REMOVED
  private final ChatClient.TestConfig testConfig;
  private final CardLayout cardLayout = new CardLayout();
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

  private final JLabel chatTitleLabel = new JLabel("Chat");
  private final JTextArea messageArea = new JTextArea(20, 56);
  private final JTextField toEmailField = new JTextField(24);
  private final JTextField messageField = new JTextField(36);
  private final JButton sendButton = new JButton("Send");

  private final JTextArea debugArea = new JTextArea(18, 36);

  /**
   * Responsibility: initialize window and attach UI listener.
   * Input: session transport and debug sidebar toggle.
   * Output: visible ready-to-use window instance.
   */
  public ChatWindow(ChatClientSession session, boolean debugSidebarEnabled, ChatClient.TestConfig testConfig) {
    super("Chat Client");
    this.session = session;
    this.debugSidebarEnabled = debugSidebarEnabled;
    this.testConfig = testConfig;
    this.session.setUiListener(this);
    buildUi();
    wireActions();
    handleAutoLogin();
  }

  /**
   * TEST ONLY - TO BE REMOVED
   * Responsibility: handle auto-login if configured.
   */
  private void handleAutoLogin() {
    if (testConfig != null && testConfig.skipLogin) {
      String email = null;
      String password = null;
      if (testConfig.selectUserA) {
        email = testConfig.userAEmail;
        password = testConfig.userAPassword;
      } else if (testConfig.selectUserB) {
        email = testConfig.userBEmail;
        password = testConfig.userBPassword;
      }

      if (email != null && password != null) {
        emailField.setText(email);
        passwordField.setText(password);
        System.out.println("[test] auto-login triggered for: " + email);
        runAuth(true);
      }
    }
  }

  /**
   * Responsibility: build auth/chat layouts and optional debug panel.
   * Input: local component state.
   * Output: composed Swing content pane.
   */
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

  /**
   * Responsibility: create authentication form view.
   * Input: auth widgets.
   * Output: assembled auth panel.
   */
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

  /**
   * Responsibility: create conversation-based chat view with list and detail panes.
   * Input: chat widgets.
   * Output: assembled chat panel.
   */
  private JPanel buildChatPanel() {
    JPanel panel = new JPanel(new BorderLayout(8, 8));
    panel.add(chatTitleLabel, BorderLayout.NORTH);

    JPanel leftPane = new JPanel(new BorderLayout(6, 6));
    leftPane.setBorder(BorderFactory.createEmptyBorder(4, 4, 4, 4));
    leftPane.add(startConversationButton, BorderLayout.NORTH);
    conversationList.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
    leftPane.add(new JScrollPane(conversationList), BorderLayout.CENTER);

    JPanel rightPane = new JPanel(new BorderLayout(8, 8));
    messageArea.setEditable(false);
    rightPane.add(new JScrollPane(messageArea), BorderLayout.CENTER);

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

  /**
   * Responsibility: bind button/list actions to session operations.
   * Input: user action events.
   * Output: auth/send/refresh behavior wiring.
   */
  private void wireActions() {
    loginButton.addActionListener(e -> runAuth(true));
    registerButton.addActionListener(e -> runAuth(false));

    sendButton.addActionListener(e -> runSend());
    messageField.addActionListener(e -> runSend());

    startConversationButton.addActionListener(
        e -> {
          conversationList.clearSelection();
          messageArea.setText("");
          chatTitleLabel.setText("Chat - New conversation");
          toEmailField.setText("");
          toEmailField.setEditable(true);
          toEmailField.requestFocusInWindow();
        });

    conversationList.addListSelectionListener(
        e -> {
          if (!e.getValueIsAdjusting()) {
            refreshMessagePaneForSelection();
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

  /**
   * Responsibility: run login/register command asynchronously.
   * Input: flag indicating login vs register.
   * Output: auth status label updates.
   */
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

  /**
   * Responsibility: send message using selected conversation or new recipient email.
   * Input: current UI fields and selected conversation.
   * Output: asynchronous send request.
   */
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

  /**
   * Responsibility: reload left pane conversation list from local DB.
   * Input: none.
   * Output: refreshed list model with recent-first ordering.
   */
  private void refreshConversationList() {
    List<DatabaseManager.ConversationSummary> conversations = session.listConversations();
    String previousSelection =
        conversationList.getSelectedValue() == null
            ? null
            : conversationList.getSelectedValue().conversationId;

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
  }

  /**
   * Responsibility: render latest 10 local messages for selected conversation.
   * Input: selected conversation item.
   * Output: updated right-pane transcript and recipient field mode.
   */
  private void refreshMessagePaneForSelection() {
    ConversationItem selected = conversationList.getSelectedValue();
    if (selected == null) {
      messageArea.setText("No conversation selected. Start a new conversation.");
      toEmailField.setEditable(true);
      return;
    }

    chatTitleLabel.setText("Chat - " + safe(selected.peerEmail));
    toEmailField.setText(safe(selected.peerEmail));
    toEmailField.setEditable(false);

    List<DatabaseManager.MessageRow> rows = session.listLatestMessages(selected.conversationId, 10);
    StringBuilder builder = new StringBuilder();
    for (DatabaseManager.MessageRow row : rows) {
      String role = "OUTBOUND".equals(row.direction()) ? "You" : safe(row.senderEmail());
      builder
          .append("[")
          .append(TIME_FMT.format(Instant.ofEpochMilli(row.sentAtMs())))
          .append("] ")
          .append(role)
          .append(": ")
          .append(row.content())
          .append(" (")
          .append(row.status())
          .append(")")
          .append(System.lineSeparator());
    }
    messageArea.setText(builder.toString());
    messageArea.setCaretPosition(messageArea.getDocument().getLength());
  }

  /**
   * Responsibility: append diagnostic line in debug sidebar when enabled.
   * Input: debug text.
   * Output: debug text area update.
   */
  private void appendDebugLine(String line) {
    if (!debugSidebarEnabled) {
      return;
    }
    debugArea.append(line + System.lineSeparator());
    debugArea.setCaretPosition(debugArea.getDocument().getLength());
  }

  /**
   * Responsibility: render info/debug callbacks from session.
   * Input: info text.
   * Output: debug sidebar update.
   */
  @Override
  public void onInfo(String text) {
    SwingUtilities.invokeLater(() -> appendDebugLine(text));
  }

  /**
   * Responsibility: reflect connection health in auth banner/debug panel.
   * Input: connected flag.
   * Output: label and debug updates.
   */
  @Override
  public void onConnectionState(boolean connected) {
    SwingUtilities.invokeLater(
        () -> {
          authConnectionLabel.setText("Server: " + (connected ? "connected" : "reconnecting"));
          appendDebugLine("[client] connection state: " + (connected ? "connected" : "reconnecting"));
        });
  }

  /**
   * Responsibility: switch between auth/chat cards when auth state changes.
   * Input: auth state payload.
   * Output: card transition and data refresh.
   */
  @Override
  public void onAuthState(boolean authenticated, String email, String error) {
    SwingUtilities.invokeLater(
        () -> {
          if (authenticated) {
            authStatusLabel.setText("Authenticated.");
            chatTitleLabel.setText("Chat - " + email);
            cardLayout.show(cardPanel, CHAT_CARD);
            refreshConversationList();
            refreshMessagePaneForSelection();
          } else {
            cardLayout.show(cardPanel, AUTH_CARD);
            if (error != null && !error.isBlank()) {
              authStatusLabel.setText("Auth failed: " + error);
            }
          }
        });
  }

  /**
   * Responsibility: react to inbound message callback by refreshing conversation UI.
   * Input: inbound message display fields.
   * Output: refreshed list and message pane.
   */
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
          refreshMessagePaneForSelection();
        });
  }

  /**
   * Responsibility: show send-ack result to user and debug panel.
   * Input: ack result fields.
   * Output: error dialog on failure and debug entry.
   */
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

  /**
   * Responsibility: refresh conversation and message panes after DB changes.
   * Input: change signal without payload.
   * Output: UI list/detail refresh.
   */
  @Override
  public void onConversationDataChanged() {
    SwingUtilities.invokeLater(
        () -> {
          refreshConversationList();
          refreshMessagePaneForSelection();
        });
  }

  /**
   * Responsibility: render server error event in debug output.
   * Input: error code and reason.
   * Output: debug sidebar line.
   */
  @Override
  public void onError(String code, String reason) {
    SwingUtilities.invokeLater(() -> appendDebugLine("ERROR code=" + code + " reason=" + reason));
  }

  /**
   * Responsibility: normalize nullable strings before UI rendering.
   * Input: nullable text.
   * Output: non-null string.
   */
  private String safe(String value) {
    return value == null ? "" : value;
  }

  /**
   * Responsibility: UI model for one conversation-list row.
   * Input: conversation summary fields from SQLite.
   * Output: displayable label with peer and preview.
   */
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
      String time = lastMessageAt > 0 ? TIME_FMT.format(Instant.ofEpochMilli(lastMessageAt)) : "--:--:--";
      return displayEmail + "  [" + time + "]  " + snippet;
    }
  }
}
