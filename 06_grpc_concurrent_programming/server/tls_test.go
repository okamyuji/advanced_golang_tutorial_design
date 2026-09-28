package server

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"

	pb "grpc-concurrent-programming/proto"
	"grpc-concurrent-programming/security"
)

// writeSelfSignedCert 127.0.0.1向けの自己署名証明書と秘密鍵をPEMで書き出し、そのパスを返します
func writeSelfSignedCert(t *testing.T) (certFile, keyFile string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "127.0.0.1"},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IsCA:                  true,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	certFile, keyFile = filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatal(err)
	}
	return certFile, keyFile
}

// TestTLS_証明書ファイルで起動し耐量子の鍵交換で接続する
func TestTLS_証明書ファイルで起動し耐量子の鍵交換で接続する(t *testing.T) {
	certFile, keyFile := writeSelfSignedCert(t)
	config := testConfig()
	config.EnableTLS = true
	config.TLSCertFile = certFile
	config.TLSKeyFile = keyFile

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv, userServer, err := NewGRPCServer(config)
	if err != nil {
		t.Fatalf("サーバー作成エラー: %v", err)
	}
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(func() {
		srv.Stop()
		userServer.Close()
	})

	creds, err := security.ClientTLSCredentials(certFile)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(creds))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	var p peer.Peer
	if _, err := pb.NewUserServiceClient(conn).GetUser(authContext(t), &pb.GetUserRequest{Id: 1}, grpc.Peer(&p)); err != nil {
		t.Fatalf("TLS経由のGetUser エラー: %v", err)
	}
	info, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok {
		t.Fatalf("AuthInfo = %T, want credentials.TLSInfo", p.AuthInfo)
	}
	if info.State.Version != tls.VersionTLS13 {
		t.Errorf("TLS version = %x, want TLS 1.3", info.State.Version)
	}
	if info.State.CurveID != tls.X25519MLKEM768 {
		t.Errorf("鍵交換 = %v, want X25519MLKEM768", info.State.CurveID)
	}
}

// TestTLS_証明書のパスが誤っていればサーバー作成がエラーになる
func TestTLS_証明書のパスが誤っていればサーバー作成がエラーになる(t *testing.T) {
	config := testConfig()
	config.EnableTLS = true
	config.TLSCertFile = filepath.Join(t.TempDir(), "missing.pem")
	config.TLSKeyFile = config.TLSCertFile
	if _, _, err := NewGRPCServer(config); err == nil {
		t.Fatal("存在しない証明書でもサーバーが作成された")
	}
}
