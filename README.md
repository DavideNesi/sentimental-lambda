# sentimental-lambda
Lambda architecture code for tweets' sentiment analysis.

## Overview

This code is designed to work with Hadoop 2.9.2 and LingPipe.

### MapReduce code

Map (Filter tweets by keyword) -> Map (Assign a sentiment value to each tweet) -> Reduce (count each sentiment).
