package com.dmathieu.kafka.opensearch;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.X509ExtendedTrustManager;
import java.net.Socket;
import java.security.cert.X509Certificate;

public final class UnsafeX509ExtendedTrustManager extends X509ExtendedTrustManager {
  private static final X509ExtendedTrustManager INSTANCE = new UnsafeX509ExtendedTrustManager();
  private static final X509Certificate[] EMPTY_CERTIFICATES = new X509Certificate[0];

  private UnsafeX509ExtendedTrustManager() {}

  public static X509ExtendedTrustManager getInstance() {
    return INSTANCE;
  }

  @Override
  public void checkClientTrusted(X509Certificate[] certificates, String authType) {
    // ignore certificate validation
  }

  @Override
  public void checkClientTrusted(X509Certificate[] certificates, String authType, Socket socket) {
    // ignore certificate validation
  }

  @Override
  public void checkClientTrusted(X509Certificate[] certificates, String authType, SSLEngine sslEngine) {
    // ignore certificate validation
  }

  @Override
  public void checkServerTrusted(X509Certificate[] certificates, String authType) {
    // ignore certificate validation
  }

  @Override
  public void checkServerTrusted(X509Certificate[] certificates, String authType, Socket socket) {
    // ignore certificate validation
  }

  @Override
  public void checkServerTrusted(X509Certificate[] certificates, String authType, SSLEngine sslEngine) {
    // ignore certificate validation
  }

  @Override
  public X509Certificate[] getAcceptedIssuers() {
    return EMPTY_CERTIFICATES;
  }
}
