#!/bin/sh
# ============================================================
# gen-certs.sh — Gera o certificado TLS auto-assinado
#
# Todos os contêineres compartilham o mesmo par cert.pem/key.pem.
# O broker usa tls.LoadX509KeyPair; drones e o tester usam
# InsecureSkipVerify: true (certificado auto-assinado no cluster interno).
#
# Execute UMA VEZ antes do primeiro docker-compose up:
#   chmod +x gen-certs.sh && ./gen-certs.sh
# ============================================================

set -e

if [ -f cert.pem ] && [ -f key.pem ]; then
  echo "[TLS] cert.pem e key.pem já existem. Nada a fazer."
  echo "      Para regenerar: rm cert.pem key.pem && ./gen-certs.sh"
  exit 0
fi

echo "[TLS] Gerando certificado auto-assinado (RSA 2048, 365 dias)..."

openssl req -x509 \
  -newkey rsa:2048 \
  -keyout key.pem \
  -out cert.pem \
  -days 365 \
  -nodes \
  -subj "/C=BR/ST=Bahia/L=Feira de Santana/O=Ormuz-P3/CN=ormuz-cluster" \
  -addext "subjectAltName=DNS:broker1,DNS:broker2,DNS:broker3,DNS:broker4,DNS:localhost"

echo "[TLS] Gerado com sucesso:"
echo "      cert.pem  — certificado público"
echo "      key.pem   — chave privada (não commitar no git!)"
echo ""
echo "Próximo passo: docker-compose up --build"
