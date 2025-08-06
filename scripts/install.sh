#!/bin/bash
"""
Installation script for Amazon Jobs Scraper
"""

set -e  # Exit on any error

echo "🚀 Installing Amazon Jobs Scraper..."

# Check if Python 3 is installed
if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 is required but not installed."
    exit 1
fi

# Check Python version
python_version=$(python3 -c 'import sys; print(".".join(map(str, sys.version_info[:2])))')
echo "✅ Python $python_version detected"

# Create virtual environment if it doesn't exist
if [ ! -d "venv" ]; then
    echo "📦 Creating virtual environment..."
    python3 -m venv venv
fi

# Activate virtual environment
echo "🔧 Activating virtual environment..."
source venv/bin/activate

# Upgrade pip
echo "⬆️  Upgrading pip..."
pip install --upgrade pip

# Install dependencies
echo "📚 Installing dependencies..."
pip install -r requirements.txt

# Install the package in development mode
echo "🔧 Installing package in development mode..."
pip install -e .

# Create necessary directories
echo "📁 Creating directories..."
mkdir -p data/{raw,processed,backups}
mkdir -p logs
mkdir -p tests

# Set up git hooks (if git is available)
if command -v git &> /dev/null; then
    echo "🔗 Setting up git hooks..."
    # Add pre-commit hook for testing
    cat > .git/hooks/pre-commit << 'EOF'
#!/bin/bash
echo "🧪 Running tests before commit..."
python -m pytest tests/ -v
EOF
    chmod +x .git/hooks/pre-commit
fi

echo "✅ Installation complete!"
echo ""
echo "🎯 Next steps:"
echo "1. Activate virtual environment: source venv/bin/activate"
echo "2. Run the scraper: python src/scripts/run_scraper.py"
echo "3. Run tests: python -m pytest tests/"
echo "4. Check health: python src/utils/health_check.py"
echo ""
echo "📖 For more information, see README.md" 