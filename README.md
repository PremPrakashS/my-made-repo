# Los Angeles COVID and Crime Data Analysis (2021-2022)

## Project Overview
This project aims to analyze and find correlations between COVID-19 data and crime data in Los Angeles for the period of 2021 to 2022. Our goal is to uncover insights into how the pandemic has impacted crime patterns and explore the interplay between public health and social dynamics in LA.

### Datasets
1. **COVID Data (2021-2022)**: This dataset includes daily counts of COVID-19 cases, deaths, and other relevant metrics across Los Angeles.
2. **Crime Data (2021-2022)**: This dataset encompasses various types of reported crimes in LA, including geographic and time-based details.

## Tools and Technologies Used
- Data Analysis: Python (Pandas)
- Visualization: Matplotlib
- Version Control: Git, GitHub

## Installation and Usage
Instructions for setting up the project environment and running the analysis scripts.

```bash
# Clone the repository
git clone https://github.com/PremPrakashS/my-made-repo.git

# Install dependencies
pip install -r requirements.txt

```

## Data Pipeline and Testing

### Data Pipeline
Our project includes an automated data pipeline that performs the following functions:
1. **Data Fetching**: Automatically retrieves the latest COVID and crime datasets from specified online sources.
2. **Data Transformation and Cleaning**: Applies a series of transformations and cleaning processes to ensure data quality and fix issues or errors in the datasets.
3. **Data Loading**: The cleaned and transformed data is then loaded into a SQL database for further analysis and querying.

This pipeline ensures our data is up-to-date and maintains integrity for reliable analysis.

### Test Script
We have developed a comprehensive test script to ensure the accuracy and efficiency of our data pipeline. The script includes tests for:
- Data fetching and loading processes.
- Data cleaning and transformation rules.
- Overall data integrity and consistency checks.

### Automated Workflow
To maintain the quality and reliability of our pipeline, we have set up an automated workflow using GitHub Actions. This workflow includes:
- **Continuous Integration Tests**: Runs our test script automatically every time there is a push to the main branch. This ensures that any new changes do not disrupt the pipeline's functionality.

This automated workflow helps in maintaining a robust and error-free data pipeline, ensuring the high quality of our project deliverables.

## How to Run the Data Pipeline and Tests
Provide detailed instructions on how to execute the data pipeline and run the test scripts. Include any necessary commands or steps to set up the environment.

```bash
# Example command to run the data pipeline
python automated_datapipeline.py

# Example command to execute the test script
python automated_testing.py
```

## Contributing
We welcome contributions to this project! If you would like to contribute, please follow these steps:
1. Fork the repository.
2. Create a new branch (`git checkout -b feature/YourFeature`).
3. Make your changes and commit them (`git commit -am 'Add some feature'`).
4. Push to the branch (`git push origin feature/YourFeature`).
5. Create a new Pull Request.

Please ensure your code is well-documented.

## Authors and Acknowledgment
This project was initiated and completed by Prem Prakash Singh. 

## Special Thanks to Our Tutors:
I would like to extend my gratitude to our tutors **Philip Heltweg** and **Georg Schwarz** for their guidance and support throughout this project. Their expertise and insights have been instrumental in shaping my approach and methodologies. This project would not have been possible without their mentorship and encouragement.

## License
This project is licensed under the CC0-1 Universal License - see the [LICENSE.md](LICENSE) file for details.


## Project Status
This project is completed.