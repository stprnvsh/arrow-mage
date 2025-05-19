import streamlit as st

def load_visualization_styles():
    """
    Load custom CSS styles for visualization components
    """
    st.markdown("""
    <style>
    /* Common card styling for visualization components */
    .data-card {
        background-color: rgba(255, 255, 255, 0.05);
        border-radius: 10px;
        padding: 15px;
        margin-bottom: 20px;
        border-left: 4px solid #2e6fdb;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
    }
    
    /* Suggestion card */
    .suggestion-card {
        background-color: rgba(97, 142, 204, 0.1);
        border-radius: 10px;
        padding: 15px;
        margin-bottom: 15px;
        border-left: 4px solid #2e6fdb;
    }
    
    /* Tabs styling */
    .visualization-tabs .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
    }
    
    .visualization-tabs .stTabs [data-baseweb="tab"] {
        height: 45px;
        white-space: pre-wrap;
        background-color: rgba(255, 255, 255, 0.05);
        border-radius: 4px 4px 0px 0px;
        gap: 1px;
        padding-top: 10px;
        padding-bottom: 10px;
    }
    
    .visualization-tabs .stTabs [aria-selected="true"] {
        background-color: rgba(97, 142, 204, 0.1);
        border-bottom: 2px solid #2e6fdb;
    }
    
    /* Code block styling */
    .code-block {
        background-color: rgba(0, 0, 0, 0.2);
        border-radius: 5px;
        padding: 10px;
        font-family: 'Courier New', monospace;
        font-size: 0.85em;
        overflow-x: auto;
    }
    
    /* SQL query styling */
    .sql-query {
        background-color: rgba(48, 51, 107, 0.1);
        border-radius: 5px;
        padding: 15px;
        font-family: 'Courier New', monospace;
        margin: 10px 0;
        border-left: 3px solid #2e6fdb;
    }
    
    /* AI badge for suggestions */
    .ai-badge {
        display: inline-block;
        padding: 3px 8px;
        background-color: #2e6fdb;
        color: white;
        border-radius: 12px;
        font-size: 0.8em;
        margin-right: 8px;
    }
    
    /* Map container */
    .map-container {
        border-radius: 10px;
        overflow: hidden;
        margin: 15px 0;
    }
    
    /* Visualization controls */
    .control-panel {
        background-color: rgba(255, 255, 255, 0.05);
        padding: 15px;
        border-radius: 10px;
        margin-bottom: 15px;
    }
    
    /* Responsive adjustments */
    @media (max-width: 768px) {
        .data-card {
            padding: 10px;
            margin-bottom: 15px;
        }
        
        .visualization-tabs .stTabs [data-baseweb="tab"] {
            height: auto;
            padding: 8px;
        }
    }
    </style>
    """, unsafe_allow_html=True)

def apply_apple_inspired_theme():
    """
    Apply Apple-inspired theme elements to visualization components
    """
    st.markdown("""
    <style>
    /* Subtle glass-like surfaces for cards */
    .apple-card {
        background: rgba(255, 255, 255, 0.05);
        backdrop-filter: blur(10px);
        -webkit-backdrop-filter: blur(10px);
        border-radius: 12px;
        padding: 20px;
        box-shadow: 0 4px 24px rgba(0, 0, 0, 0.1);
        margin-bottom: 24px;
    }
    
    /* Smooth, rounded buttons */
    .apple-button {
        background: rgba(0, 122, 255, 0.8);
        border-radius: 8px;
        padding: 8px 16px;
        color: white;
        font-weight: 500;
        border: none;
        transition: all 0.3s ease;
    }
    
    .apple-button:hover {
        background: rgba(0, 122, 255, 1);
        box-shadow: 0 4px 12px rgba(0, 122, 255, 0.3);
    }
    
    /* Typography adjustments */
    .apple-heading {
        font-weight: 500;
        letter-spacing: -0.5px;
        margin-bottom: 16px;
    }
    
    .apple-subheading {
        font-weight: 400;
        color: rgba(255, 255, 255, 0.8);
        letter-spacing: -0.3px;
        margin-bottom: 12px;
    }
    
    /* Active component indicator */
    .apple-active-indicator {
        background: #2e6fdb;
        border-radius: 4px;
        padding: 2px 8px;
        color: white;
        font-size: 0.8em;
        margin-left: 8px;
    }
    
    /* Card hover effect */
    .apple-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 8px 30px rgba(0, 0, 0, 0.12);
        transition: all 0.3s ease;
    }
    
    /* Minimize scrollbars except when actively scrolling */
    ::-webkit-scrollbar {
        width: 6px;
        height: 6px;
    }
    
    ::-webkit-scrollbar-track {
        background: transparent;
    }
    
    ::-webkit-scrollbar-thumb {
        background: rgba(255, 255, 255, 0.2);
        border-radius: 6px;
    }
    
    ::-webkit-scrollbar-thumb:hover {
        background: rgba(255, 255, 255, 0.3);
    }
    </style>
    """, unsafe_allow_html=True) 