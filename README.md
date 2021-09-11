# Advanced-House-Price-Prediction using Spark, Kafka
It is a Advanced Problem of Regression which requires advanced techniques of feature engineering, feature selection and extraction, modelling, model evaluation, and Statistics.

Start here if...
You have some experience with R or Python and machine learning basics. This is a perfect competition for data science students who have completed an online course in machine learning and are looking to expand their skill set before trying a featured competition. 

Application which uses machine learning and regression algorithms to predict home prices, used as a term project for a master's course in Big Data and Machine Learning. The goal is to demonstrate the usefulness of various regression algorithms in Spark's machine learning library, MLlib. To help demonstrate real-world effectiveness Kaggle learning competition output from various techniques is entered into the competition, resulting in increasing or decreasing accuracy scores. The course for this project focused on Big Data analysis using Hadoop and Spark, so an additional requirement was given to generate predictions using regression techniques that would scale to large datasets on a Spark cluster.

# Description


Ask a home buyer to describe their dream house, and they probably won't begin with the height of the basement ceiling or the proximity to an east-west railroad. But this playground competition's dataset proves that much more influences price negotiations than the number of bedrooms or a white-picket fence.

With 79 explanatory variables describing (almost) every aspect of residential homes in Ames, Iowa, this competition challenges you to predict the final price of each home.

Practice Skills
Creative feature engineering 
Advanced regression techniques like random forest and gradient boosting
Acknowledgments
The Ames Housing dataset was compiled by Dean De Cock for use in data science education. It's an incredible alternative for data scientists looking for a modernized and expanded version of the often cited Boston Housing dataset. 

# Goal
It is your job to predict the sales price for each house. For each Id in the test set, you must predict the value of the SalePrice variable. 

# Metric
Submissions are evaluated on Root-Mean-Squared-Error (RMSE) between the logarithm of the predicted value and the logarithm of the observed sales price. (Taking logs means that errors in predicting expensive houses and cheap houses will affect the result equally.)

Training and Test Data
The training and test data given consists of housing data provided by Kaggle. The data has 79 features, with the addition of Sale Price included on the training data.

The data is much cleaner than typical real-world data. Only missing value handline was required. After discussion with the instructor, the recommendation was to calculate and fill missing numeric values with the column mean.

https://github.com/mkbehbehani/spark-advanced-regression-kaggle/blob/master/source-data/train.csv












 
