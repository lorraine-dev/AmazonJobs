#!/usr/bin/env python3
"""
Verification script for Amazon Jobs Scraper project structure
"""

import os
import sys
from pathlib import Path

def check_directory_structure():
    """Verify the project directory structure."""
    
    print("🔍 Verifying Amazon Jobs Scraper Project Structure")
    print("=" * 60)
    
    # Expected structure
    expected_structure = {
        # Core source code
        'src/__init__.py': 'Main package init',
        'src/scraper/__init__.py': 'Scraper package init',
        'src/scraper/amazon_scraper.py': 'Main scraper class',
        'src/scraper/config.py': 'Configuration management',
        'src/utils/__init__.py': 'Utils package init',
        'src/utils/health_check.py': 'Health monitoring',
        'src/utils/logging_utils.py': 'Logging utilities',
        'src/scripts/__init__.py': 'Scripts package init',
        'src/scripts/run_scraper.py': 'Main execution script',
        
        # Configuration
        'config/scraper_config.yaml': 'Scraper configuration',
        
        # Data directories
        'data/raw/': 'Raw data directory',
        'data/processed/': 'Processed data directory',
        'data/backups/': 'Backup directory',
        
        # Documentation and examples
        'docs/reference/': 'Reference documentation',
        'examples/basic_usage.py': 'Usage example',
        'notebooks/development/': 'Development notebooks',
        
        # Testing
        'tests/__init__.py': 'Tests package init',
        'tests/test_scraper.py': 'Scraper tests',
        
        # Deployment
        'deployment/cron/crontab.txt': 'Cron configuration',
        'deployment/docker/Dockerfile': 'Docker configuration',
        'deployment/docker/docker-compose.yml': 'Docker compose',
        
        # Project files
        'setup.py': 'Package setup',
        'requirements.txt': 'Dependencies',
        '.gitignore': 'Git ignore rules',
        'README.md': 'Project documentation',
        'ORGANIZATION_SUMMARY.md': 'Organization summary',
        
        # Scripts
        'scripts/install.sh': 'Installation script',
        'scripts/verify_structure.py': 'This verification script',
    }
    
    # Check each expected file/directory
    all_good = True
    for path, description in expected_structure.items():
        if os.path.exists(path):
            status = "✅"
        else:
            status = "❌"
            all_good = False
        
        print(f"{status} {path:<35} {description}")
    
    print("\n" + "=" * 60)
    
    if all_good:
        print("🎉 All files and directories are in place!")
        return True
    else:
        print("⚠️  Some files or directories are missing.")
        return False

def check_python_imports():
    """Test that Python imports work correctly."""
    
    print("\n🔍 Testing Python imports...")
    
    # Add src to Python path
    src_path = Path(__file__).parent.parent / "src"
    sys.path.insert(0, str(src_path))
    
    try:
        # Test basic imports
        from scraper.config import ScraperConfig
        print("✅ ScraperConfig import successful")
        
        from scraper.amazon_scraper import AmazonJobsScraper
        print("✅ AmazonJobsScraper import successful")
        
        from utils.logging_utils import setup_logging
        print("✅ Logging utilities import successful")
        
        from utils.health_check import check_scraper_health
        print("✅ Health check import successful")
        
        print("🎉 All imports working correctly!")
        return True
        
    except ImportError as e:
        print(f"❌ Import error: {e}")
        return False

def check_configuration():
    """Test configuration loading."""
    
    print("\n🔍 Testing configuration...")
    
    try:
        from scraper.config import ScraperConfig
        
        config = ScraperConfig()
        
        # Test basic config values
        max_workers = config.get('scraper.max_workers')
        if max_workers == 3:
            print("✅ Configuration loading successful")
            return True
        else:
            print(f"❌ Unexpected config value: {max_workers}")
            return False
            
    except Exception as e:
        print(f"❌ Configuration error: {e}")
        return False

def check_data_files():
    """Check that data files are in the right place."""
    
    print("\n🔍 Checking data files...")
    
    data_files = [
        'data/raw/amazon_luxembourg_jobs.csv',
        'data/raw/amazon_luxembourg_jobs_optimized.csv',
        'data/raw/amazon_luxembourg_jobs_collect_first.csv',
    ]
    
    all_good = True
    for file_path in data_files:
        if os.path.exists(file_path):
            size = os.path.getsize(file_path)
            print(f"✅ {file_path} ({size:,} bytes)")
        else:
            print(f"❌ {file_path} - Missing")
            all_good = False
    
    return all_good

def main():
    """Run all verification checks."""
    
    print("🚀 Amazon Jobs Scraper - Structure Verification")
    print("=" * 60)
    
    # Run all checks
    checks = [
        ("Directory Structure", check_directory_structure),
        ("Python Imports", check_python_imports),
        ("Configuration", check_configuration),
        ("Data Files", check_data_files),
    ]
    
    results = []
    for name, check_func in checks:
        try:
            result = check_func()
            results.append((name, result))
        except Exception as e:
            print(f"❌ {name} check failed: {e}")
            results.append((name, False))
    
    # Summary
    print("\n" + "=" * 60)
    print("📊 Verification Summary:")
    print("=" * 60)
    
    passed = 0
    total = len(results)
    
    for name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status} {name}")
        if result:
            passed += 1
    
    print(f"\n🎯 Results: {passed}/{total} checks passed")
    
    if passed == total:
        print("🎉 All verifications passed! Project structure is correct.")
        return 0
    else:
        print("⚠️  Some verifications failed. Please check the issues above.")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 