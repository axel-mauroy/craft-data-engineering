# ==============================================================================
# DOCKER MULTI-STAGE BUILD POUR DATA PIPELINE (Expert Python/uv)
# ==============================================================================
# Ce Dockerfile illustre comment minimiser la taille de l'image finale et
# sécuriser l'exécution en séparant les outils de build du runtime.

# --- ÉTAPE 1 : BUILDER (Environnement de compilation) ---
FROM python:3.12-slim-bookworm AS builder

# Installation de 'uv' pour une gestion ultra-rapide des dépendances
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Définition du répertoire de travail
WORKDIR /app

# On ne copie que les fichiers de dépendances d'abord (cache Docker optimal)
COPY pyproject.toml uv.lock ./

# Installation des dépendances dans un environnement virtuel isolé
# --no-dev : On exclut les outils de test/lint pour la prod
RUN uv sync --frozen --no-dev --no-install-project

# --- ÉTAPE 2 : RUNTIME (Image finale légère & sécurisée) ---
FROM python:3.12-slim-bookworm AS runtime

# Création d'un utilisateur non-root pour la sécurité (Principle of Least Privilege)
RUN groupadd -r datauser && useradd -r -g datauser datauser

WORKDIR /app

# Récupération de l'environnement virtuel depuis le builder
# C'est ici que la magie du multi-stage opère : on ne garde que le nécessaire.
COPY --from=builder /app/.venv /app/.venv

# Copie du code source uniquement
COPY src/ ./src/

# Configuration des variables d'environnement
ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONUNBUFFERED=1

# Changement d'utilisateur
USER datauser

# Commande d'entrée (ex: lancement d'un job dbt ou d'un script python)
ENTRYPOINT ["python", "-m", "src.main"]


# ==============================================================================
# POURQUOI CETTE APPROCHE ?
# 1. Sécurité : Aucun compilateur (gcc) ou outil de build dans l'image finale.
# 2. Performance : Image finale < 100MB (contre 500MB+ en image standard).
# 3. Cache : Les dépendances ne sont réinstallées que si pyproject.toml change.
# ==============================================================================
