# WP3-KPI-calculator

This tool, developed by the Athena Research Center, generates JSON files that contain calculated Key Performance Indicators (KPIs). These KPIs are formatted to be compatible with the Stiviewer application, ensuring seamless uploads and integration.

The tool, primarily operates through the Climate_EU.py script. To run the script, use the following command:
python Climate_EU.py config.json

Alternatively, you can execute it within a Docker container.

**Important:** A copy of the OpenAIRE Research Graph should be available for the script execution.

# Commands to Build and Run the Docker Container

## Creating a Docker Image

To create a Docker image, use the following command. The -t flag allows you to specify the image name and optionally tag it with a version:

docker build -t NAME:tag <Dockerfile location>

## Example:

docker build --tag kpi-calculator .

## Running the Docker Image

To run a Docker container using the created image, apply the following command:

docker run --rm -i --name kpi_calculation python Climate_EU.py --config="config.json"

![This project has received funding from the European Union’s Horizon 2020 research and innovation programme under grant agreement No. 101004870. H2020-SC6-GOVERNANCE-2018-2019-2020 / H2020-SC6-GOVERNANCE-2020](https://github.com/IntelCompH2020/.github/blob/main/profile/banner.png)
