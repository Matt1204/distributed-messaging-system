package com.coen6731.chat.client;

import java.awt.BorderLayout;
import java.awt.CardLayout;
import java.awt.FlowLayout;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.CompletableFuture;
import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JPasswordField;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.SwingUtilities;
import javax.swing.WindowConstants;

public class ChatWindow extends JFrame implements ClientUiListener {
  private static final DateTimeFormatter TIME_FMT = DateTimeFormatter.ofPattern("HH:mm:ss")
      .withZone(ZoneId.systemDefault());
  private static final String AUTH_CARD = "auth";
  private static final String CHAT_CARD = "chat";

  private final ChatClientSession session;
  private final boolean debugSidebarEnabled;
  private final CardLayout cardLayout = new CardLayout();
  private final JPanel cardPanel = new JPanel(cardLayout);

  private final JTextField emailField = new JTextField(20);
  private final JPasswordField passwordField = new JPasswordField(20);
  private final JButton loginButton = new JButton("Login");
  private final JButton registerButton = new JButton("Register");
  private final JLabel authStatusLabel = new JLabel("Please login or register.");
  private final JLabel authConnectionLabel = new JLabel("Server: connecting");

  private final JTextField toEmailField = new JTextField(20);
  private final JTextField messageField = new JTextField(40);
  private final JButton sendButton = new JButton("Send");
  private final JLabel chatTitleLabel = new JLabel("Chat");

  private final JTextArea chatArea = new JTextArea(18, 80);
  private final JTextArea debugArea = new JTextArea(18, 36);

  public ChatWindow(ChatClientSession session, boolean debugSidebarEnabled) {
    super("Chat Client");
    this.session = session;
    this.debugSidebarEnabled = debugSidebarEnabled;
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
    chatArea.setEditable(false);
    panel.add(chatTitleLabel, BorderLayout.NORTH);
    panel.add(new JScrollPane(chatArea), BorderLayout.CENTER);

    JPanel sendPanel = new JPanel(new FlowLayout(FlowLayout.LEFT));
    sendPanel.add(new JLabel("To:"));
    sendPanel.add(toEmailField);
    sendPanel.add(new JLabel("Message:"));
    sendPanel.add(messageField);
    sendPanel.add(sendButton);
    panel.add(sendPanel, BorderLayout.SOUTH);
    return panel;
  }

  private void wireActions() {
    loginButton.addActionListener(e -> runAuth(true));
    registerButton.addActionListener(e -> runAuth(false));

    sendButton.addActionListener(e -> runSend());
    messageField.addActionListener(e -> runSend());

    addWindowListener(new java.awt.event.WindowAdapter() {
      @Override
      public void windowClosed(java.awt.event.WindowEvent e) {
        session.close();
      }
    });
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
    CompletableFuture
        .supplyAsync(() -> login ? session.login(email, password) : session.register(email, password))
        .whenComplete((ok, err) -> SwingUtilities.invokeLater(() -> {
          if (err != null) {
            authStatusLabel.setText("Auth failed: " + err.getMessage());
            return;
          }
          if (!Boolean.TRUE.equals(ok)) {
            String authErr = session.getLastAuthError();
            authStatusLabel.setText("Auth failed: " + (authErr == null ? "unknown error" : authErr));
          }
        }));
  }

  private void runSend() {
    String toEmail = toEmailField.getText().trim();
    String text = messageField.getText().trim();

    if (toEmail.isEmpty() || text.isEmpty()) {
      JOptionPane.showMessageDialog(this, "Usage: /send <toEmail> <text>", "Send Message",
          JOptionPane.WARNING_MESSAGE);
      return;
    }

    CompletableFuture.runAsync(() -> session.sendMessage(toEmail, text));
    messageField.setText("");
  }

  private void appendChatLine(String line) {
    chatArea.append(line + System.lineSeparator());
    chatArea.setCaretPosition(chatArea.getDocument().getLength());
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
    SwingUtilities.invokeLater(() -> {
      authConnectionLabel.setText("Server: " + (connected ? "connected" : "reconnecting"));
      appendDebugLine("[client] connection state: " + (connected ? "connected" : "reconnecting"));
    });
  }

  @Override
  public void onAuthState(boolean authenticated, String email, String error) {
    SwingUtilities.invokeLater(() -> {
      if (authenticated) {
        chatTitleLabel.setText("Chat - " + email);
        authStatusLabel.setText("Authenticated.");
        cardLayout.show(cardPanel, CHAT_CARD);
      } else {
        cardLayout.show(cardPanel, AUTH_CARD);
        if (error != null && !error.isBlank()) {
          authStatusLabel.setText("Auth failed: " + error);
        }
      }
    });
  }

  @Override
  public void onChatMessage(String fromEmail, String text, String msgId, long ts) {
    SwingUtilities.invokeLater(() -> {
      String formatted = "[" + TIME_FMT.format(Instant.ofEpochMilli(ts)) + "] "
          + fromEmail + ": " + text + " (" + msgId + ")";
      appendChatLine(formatted);
    });
  }

  @Override
  public void onError(String code, String reason) {
    SwingUtilities.invokeLater(() -> appendDebugLine("ERROR code=" + code + " reason=" + reason));
  }
}
