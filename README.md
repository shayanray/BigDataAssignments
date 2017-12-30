# Big Data assignments
Assignments as given in the course of CSE545-BigData Analytics

### Blog Corpus Industry Mention count using Spark:
Assignment 1: (a1p2_ray.py)
Dataset: http://u.cs.biu.ac.il/~koppel/BlogCorpus.htm

TO-DO: Parse the blog corpus to:
  - Find all the industries
  - Find the number of times a industry was mentioned in all the blogs


### Satellite Image Analysis to find similar regions using Spark and AWS
Assignment 2:(a2_ray.py)
Dataset: https://lta.cr.usgs.gov/high_res_ortho

TO-DO: Find similar regions in Long island using satellite images on AWS cluster.

  - Preprocess the data to split and reduce the resolution of the images, flatten them, calculate the intensity and clip the image values.
  - Implement Locality Sensitive Hashing(LSH) to find similar regions.
  - Implement dimensionality reduction(SVD) to reduce the size of the images.
  

