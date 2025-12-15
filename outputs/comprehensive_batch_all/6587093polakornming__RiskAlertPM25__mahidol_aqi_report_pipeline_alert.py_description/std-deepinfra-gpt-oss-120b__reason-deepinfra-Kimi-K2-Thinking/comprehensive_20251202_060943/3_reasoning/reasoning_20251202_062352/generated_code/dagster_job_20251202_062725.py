@op
def fetch_mahidol_aqi_html(context, config: FetchConfig) -> str:
    """Returns path to saved HTML file"""

@op
def extract_and_validate_data(context, html_path: str, config: ExtractConfig) -> Optional[str]:
    """Returns path to JSON file if data is new, else None"""

@op
def load_data_to_postgresql(context, json_path: Optional[str], config: LoadConfig):
    """Loads data if json_path is not None"""

@op
def conditional_email_alert(context, config: AlertConfig):
    """Checks AQI and sends alert if needed"""