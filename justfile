# justfile for craft-data-engineering automation

# Default recipe: list available recipes
default:
    @just --list

# Setup: Create directory structure and initial files
setup:
    @echo "Creating directory structure..."
    mkdir -p src tests data notebooks
    touch src/.gitkeep tests/.gitkeep data/.gitkeep notebooks/.gitkeep
    @echo "Initializing Git..."
    @if [ ! -d .git ]; then \
        git init; \
    fi
    @echo "Creating initial files if they don't exist..."
    @if [ ! -f .gitignore ]; then \
        printf "# Python\n__pycache__/\n.venv/\n\n# Mac\n.DS_Store\n\n# IDE\n.vscode/\n.idea/\n" > .gitignore; \
    fi
    @if [ ! -f README.md ]; then \
        printf "# craft-data-engineering\n\nWelcome to the craft-data-engineering repository.\n\n## Structure\n- \`src/\`: Source code\n- \`tests/\`: Unit tests\n- \`data/\`: Sample data\n- \`notebooks/\`: Exploration\n" > README.md; \
    fi

# Create-repo: Create the GitHub repository and push current state
create-repo name="craft-data-engineering":
    @echo "Creating GitHub repository {{name}}..."
    gh repo create {{name}} --public --source=. --remote=origin --push || echo "Repository might already exist or push failed."

# Clean: Remove temporary files and caches
clean:
    @echo "Cleaning up..."
    find . -type d -name "__pycache__" -exec rm -rf {} +
    find . -name ".DS_Store" -delete
    @echo "Clean complete."
