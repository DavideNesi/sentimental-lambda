# sentimental-lambda
Lambda architecture code for tweets' sentiment analysis.

In collaboration with Davide Nesi.

## Overview

This code is designed to work with Hadoop 2.9.2 and LingPipe.

### MapReduce code

Map (Filter tweets by keyword) -> Map (Assign a sentiment value to each tweet) -> Reduce (count each sentiment).

Usage:
Generate the `.jar` archive, then

    hadoop jar artifact-name.jar /input/dir/path /output/dir/path ["keyword"]
    
If `keyword` is entered, only tweets that contains `keyword` will be counted.
