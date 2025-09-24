# Insurance Fraud Detection

<!-- ABOUT THE PROJECT -->
## 📝 About The Project

This project explores the development of an R-based data analysis and visualization tool for detecting insurance fraud. The end goal is to provide users with an interactive environment where they can upload their own datasets, generate insights, and evaluate predictive models in a transparent and user-friendly way.

This project includes a preprocessing pipeline to clean and standardize datasets so that they are suitable for input to a pre-trained model (complete with its own feature engineering).The model will then detect potential cases of insurance fraud at both the aggregate and individual level, complete with visualisations and interactive tools that allow users to gain deeper insights into the underlying patterns within their data.

> **NOTE**: This project is currently in the prototype stage and is unfinished, which means it may contain incomplete features, limited functionality, and potential bugs. It should be considered a work in progress rather than a polished final product.
> Some _**Planned changes**_ include:
>
> **1.** Create an R shiny application to recieve user data, and generate visualisations and interactive functionality accordingly, examples: a **Single-observation prediction inspector** _(view model predictions and contributing factors for a selected data point)_, an **EDA dashboard**), and **what-if analysis tools**
>
> **2.** Write a report on the model, including how data was prepared, what feature engineering took place, and what model was chosen and why.
>
> **3.** Expand the model’s capabilities by introducing **hybrid anomaly detection**: combining _supervised fraud classification_ with _unsupervised methods_ (Isolation Forests, Autoencoders, and K-Means Clustering) to flag both _known fraud patterns_ and _unexpected anomalies_, and then analyzing the consistency between both approaches.  

<!-- DATA SOURCES> -->
## Data Sources

- [**fraud_oracle.csv**](https://figshare.com/articles/dataset/fraud_oracle_csv/24994233?file=44033394) (training dataset, synthetic)
- [**insurance_claims.csv**](https://data.mendeley.com/datasets/992mh7dk9y/2) (training dataset based on real-world data)
- [**actual_motor_insurance_portfolio**](https://data.mendeley.com/datasets/5cxyb5fp4f/2) (default input dataset)
