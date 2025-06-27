# YFinance - Windsurf Development Guide

Welcome to the YFinance project! This guide is specifically designed for Windsurf users to get started quickly and efficiently.

## 🚀 Quick Start

1. **Clone the repository**
```bash
git clone https://github.com/sudhirig/yfinance.git
```

2. **Open in Windsurf**
- Open Windsurf
- Click "Open Folder" or use `Cmd+O`
- Select the cloned repository folder

3. **Run Setup Script**
- Open Windsurf Terminal (View -> Terminal or `Cmd+J`)
- Navigate to project root
- Run:
```bash
python setup_windsurf.py
```

4. **Edit Database Credentials**
- Open `.env` file
- Update database credentials:
```bash
PGUSER=your_neon_user
PGPASSWORD=your_neon_password
```

5. **Access Application**
- The Flask server starts automatically
- Open browser to: http://localhost:5050

## 🛠️ Development Features

### Windsurf Integration
- Python debugger
- Code completion
- Git integration
- Terminal multiplexing
- File explorer
- AI suggestions

### Project Structure
```
yfinance/
├── app.py              # Main Flask application
├── database_config.py  # Database configuration
├── static/             # Frontend assets
├── templates/          # HTML templates
└── yfinance/           # Python modules
```

## 📚 Key Files

### 1. Configuration
- `.env` - Database credentials (DO NOT commit)
- `.env.example` - Template for `.env`
- `setup_windsurf.py` - Automated setup script

### 2. Development
- `app.py` - Main application
- `database_config.py` - Database connection
- `yfinance_nse_downloader.py` - Data ingestion

## 🛠️ Development Workflow

1. **Create Feature Branch**
```bash
git checkout -b feature/your-feature
```

2. **Make Changes**
- Use Windsurf's built-in terminal
- Leverage code completion
- Use debugger for testing

3. **Test Changes**
- Run tests in Windsurf terminal
- Use debugger for troubleshooting

4. **Commit Changes**
- Use Windsurf's Git integration
- Push changes to remote

## 🚦 Troubleshooting

### Common Issues
1. **Database Connection**
- Verify credentials in `.env`
- Run `python setup_windsurf.py --test-db`

2. **Server Not Starting**
- Check Windsurf terminal for errors
- Run `python app.py` manually

3. **Missing Dependencies**
- Run `pip install -r requirements.txt`

## 📝 Notes
- Never commit `.env` file
- Use feature branches for development
- Follow code style guidelines
- Keep database credentials secure

## 🔗 Resources
- [Windsurf Documentation](https://docs.codeium.com/windsurf)
- [Flask Documentation](https://flask.palletsprojects.com/)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
