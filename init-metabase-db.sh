#!/bin/bash

# Espera o PostgreSQL estar pronto
echo "Aguardando PostgreSQL estar pronto..."
until PGPASSWORD=metabase123 psql -h postgres-metabase -U metabase -d metabase -c '\q'; do
  echo "PostgreSQL não está pronto - esperando..."
  sleep 5
done

echo "PostgreSQL está pronto!"

# Verificar se o banco de dados do Metabase já existe
if PGPASSWORD=metabase123 psql -h postgres-metabase -U metabase -lqt | cut -d \| -f 1 | grep -qw metabase; then
    echo "Banco de dados 'metabase' já existe."
else
    echo "Criando banco de dados 'metabase'..."
    PGPASSWORD=metabase123 psql -h postgres-metabase -U metabase -c "CREATE DATABASE metabase;"
    echo "Banco de dados 'metabase' criado com sucesso."
fi

echo "Configuração do banco de dados concluída!" 