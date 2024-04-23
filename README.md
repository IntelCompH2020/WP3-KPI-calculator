# WP3-KPI-calculator

This tool, developed by the Athena Research Center, generates JSON files that contain calculated Key Performance Indicators (KPIs). These KPIs are formatted to be compatible with the Stiviewer application, ensuring seamless uploads and integration.

The tool, primarily operates through the Climate_EU.py script. To run the script, use the following command:
python Climate_EU.py config.json

# Create Docker image

sudo docker build --tag WP3-KPI-calculator -f ./dockerfile .


This tool, developed by the Athena Research Center, 

Alternatively, you can execute it within a Docker container.