package com.coen6731.chat.server;

import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;

public class UserIdInterceptor implements ServerInterceptor {
  // Define a key to access the value in the Context later
  public static final Context.Key<String> USER_ID_CTX_KEY = Context.key("userId");

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
      ServerCall<ReqT, RespT> call,
      Metadata headers,
      ServerCallHandler<ReqT, RespT> next) {

    // Read the header
    String userId = headers.get(Metadata.Key.of("x-user-id", Metadata.ASCII_STRING_MARSHALLER));

    // Allow anonymous stream setup. Authentication is enforced at event handling level.
    Context context = Context.current().withValue(USER_ID_CTX_KEY, userId);
    return Contexts.interceptCall(context, call, headers, next);
  }
}
