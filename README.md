# AWS-MR-ML
AWS Map Reduce pattern with Machine learning.
## General information:
In this project we've used amazon map reduce mechanism + machine learning in order to detect Hypernym.
The whole project is based on “Learning syntactic patterns for automatic hypernym discovery” paper written by by R. Snow, D. Jurafsky and A. Ng.
We've also used google syntactic ngrams as input + known Hypernym as a train list.
We comibned both in a specific way that allowed us to create a feature vector for each pair of nouns, using WEKA for the machine learning part.
For more info please check "DesignDocument.docx"

