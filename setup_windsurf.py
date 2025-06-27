import os
import subprocess
import sys

print("\n=== YFinance Windsurf Setup Script ===")

# Check if we're in the correct directory
if not os.path.exists('app.py'):
    print("Error: Please run this script from the project root directory")
    sys.exit(1)

# Check if .env file exists
env_file = '.env'
if not os.path.exists(env_file):
    print("\nCreating .env file from template...")
    if not os.path.exists('.env.example'):
        print("Error: .env.example template not found!")
        sys.exit(1)
    
    # Copy .env.example to .env
    with open('.env.example', 'r') as f:
        env_template = f.read()
    
    with open(env_file, 'w') as f:
        f.write(env_template)
    
    print(f"Created {env_file} from template")
    print("Please edit the credentials in {env_file} before proceeding")

# Check Python version
print("\nChecking Python version...")
python_version = sys.version_info
if python_version.major < 3 or (python_version.major == 3 and python_version.minor < 8):
    print("Error: Python 3.8 or higher is required")
    sys.exit(1)
print(f"Python {python_version.major}.{python_version.minor} detected - OK")

# Install dependencies
print("\nInstalling dependencies...")
try:
    subprocess.run(['pip', 'install', '-r', 'requirements.txt'], check=True)
    print("Dependencies installed successfully")
except subprocess.CalledProcessError as e:
    print(f"Error installing dependencies: {e}")
    sys.exit(1)

# Check database connection
print("\nTesting database connection...")
try:
    import psycopg2
    from database_config import get_db_connection
    
    conn = get_db_connection()
    if conn:
        print("Database connection successful!")
        conn.close()
    else:
        print("Warning: Database connection failed. Please check your credentials in .env file")
except Exception as e:
    print(f"Error testing database connection: {e}")

print("\n=== Setup Complete ===")
print("\nStarting Flask server...")

try:
    # Start Flask server in a new process
    import multiprocessing
    import time
    
    def start_flask():
        import os
        os.environ['FLASK_APP'] = 'app.py'
        os.environ['FLASK_ENV'] = 'development'
        os.environ['FLASK_DEBUG'] = '1'
        os.environ['PORT'] = '5050'
        import flask
        from app import app
        app.run(port=5050)
    
    # Start Flask in a separate process
    flask_process = multiprocessing.Process(target=start_flask)
    flask_process.start()
    
    # Wait a moment for the server to start
    time.sleep(2)
    
    print("\n=== YFinance is now running ===")
    print("Access the app at: http://localhost:5050")
    print("\n=== Setup Instructions ===")
    print("1. Edit .env file with your database credentials")
    print("2. The Flask server is already running")
    print("3. Access the app at: http://localhost:5050")
    print("\n=== Development Features ===")
    print("- Flask debug mode is enabled")
    print("- Auto-reload on code changes")
    print("- Database connection is configured")
    print("\n=== Troubleshooting ===")
    print("If you encounter any issues:")
    print("1. Check the .env file for correct credentials")
    print("2. Review the database connection status")
    print("3. Run 'python setup_windsurf.py --test-db' to test database connection")
    
except Exception as e:
    print(f"\nError starting Flask server: {e}")
    print("\nYou can start the server manually with: python app.py")
    sys.exit(1)
