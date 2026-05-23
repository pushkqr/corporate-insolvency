from nicegui import ui

def apply_global_styles():
    # Force dark mode globally using NiceGUI's built-in tool
    ui.dark_mode().enable()
    
    # Inject Global CSS variables for Matrix Theme
    ui.add_head_html("""
        <link rel="preconnect" href="https://fonts.googleapis.com">
        <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
        <link href="https://fonts.googleapis.com/css2?family=Share+Tech+Mono&display=swap" rel="stylesheet">
        
        <style>
            :root {
                --bg-gradient-start: #000000;
                --bg-gradient-end: #001100;
                --card-bg: rgba(0, 20, 0, 0.85);
                --card-border: rgba(0, 255, 65, 0.4);
                --accent-color: #00FF41;
                --text-main: #00FF41;
                --text-muted: #008F11;
            }

            body {
                font-family: 'Share Tech Mono', monospace !important;
                background: linear-gradient(180deg, var(--bg-gradient-start), var(--bg-gradient-end));
                background-attachment: fixed;
                color: var(--text-main) !important;
                margin: 0;
                min-height: 100vh;
            }
            
            /* Matrix generic classes */
            .glass-card {
                background: var(--card-bg) !important;
                border: 1px solid var(--card-border) !important;
                border-radius: 4px !important;
                box-shadow: 0 0 15px rgba(0, 255, 65, 0.1) !important;
                transition: transform 0.2s ease, box-shadow 0.2s ease !important;
            }

            .glass-card:hover {
                box-shadow: 0 0 25px rgba(0, 255, 65, 0.3) !important;
            }

            /* Custom Typography */
            h1, h2, h3 {
                font-weight: normal !important;
                text-shadow: 0 0 10px rgba(0, 255, 65, 0.5);
            }
            
            .gradient-text {
                color: var(--accent-color) !important;
                text-shadow: 0 0 15px rgba(0, 255, 65, 0.8);
            }

            /* Input Fields overrides */
            .q-field__control {
                background: rgba(0, 20, 0, 0.9) !important;
                border: 1px solid var(--card-border) !important;
                border-radius: 2px !important;
                padding: 0 16px !important;
            }
            .q-field__control:hover {
                border-color: var(--accent-color) !important;
            }
            .q-field__native {
                color: var(--text-main) !important;
            }

            /* Premium Buttons */
            .q-btn {
                border-radius: 2px !important;
                border: 1px solid var(--accent-color) !important;
                background: rgba(0, 255, 65, 0.1) !important;
                color: var(--accent-color) !important;
                font-weight: bold !important;
                text-transform: uppercase !important;
                letter-spacing: 0.1em !important;
                transition: all 0.2s ease !important;
            }
            .q-btn:hover {
                background: var(--accent-color) !important;
                color: #000 !important;
                box-shadow: 0 0 20px rgba(0, 255, 65, 0.6) !important;
            }

            /* Native overlay styling */
            #custom-loader {
                position: fixed; top: 0; left: 0; width: 100vw; height: 100vh;
                background: rgba(0, 0, 0, 0.85);
                z-index: 99999;
                display: none; flex-direction: column;
                justify-content: center; align-items: center;
            }
            
            #custom-loader .loader-container {
                background: rgba(0, 20, 0, 0.9);
                border: 1px solid var(--accent-color);
                padding: 40px 60px; border-radius: 4px;
                box-shadow: 0 0 30px rgba(0, 255, 65, 0.4);
                display: flex; flex-direction: column; align-items: center;
            }
            
            #custom-loader .loader {
                width: 60px; height: 60px;
                border: 4px solid transparent;
                border-top: 4px solid var(--accent-color);
                border-right: 4px solid var(--accent-color);
                border-radius: 50%;
                animation: spin 1s linear infinite; 
                margin-bottom: 20px;
            }
            
            #custom-loader .loader-text { 
                font-size: 1.25em; 
                font-family: 'Share Tech Mono', monospace;
                color: var(--accent-color); 
                text-shadow: 0 0 10px rgba(0, 255, 65, 0.6);
                letter-spacing: 0.1em;
            }
            
            @keyframes spin { 0% { transform: rotate(0deg); } 100% { transform: rotate(360deg); } }
        </style>
    """)

    # Inject the HTML for the loader directly into the body
    ui.add_body_html("""
        <div id="custom-loader">
            <div class="loader-container">
                <div class="loader"></div>
                <div class="loader-text">ACCESSING MAINFRAME...</div>
            </div>
        </div>
    """)

def show_loader():
    ui.run_javascript("document.getElementById('custom-loader').style.display = 'flex';")

def hide_loader():
    ui.run_javascript("document.getElementById('custom-loader').style.display = 'none';")
