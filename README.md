# DataPlatform-Retail-AlertGeneration

[![Build status](https://acosta-it.visualstudio.com/Data-Platform/_apis/build/status/Retail%20Alert%20Generation%20-%20CI)](https://acosta-it.visualstudio.com/Data-Platform/_build/latest?definitionId=35)

## Overview
Alert Generation is the first component in the Dynamic Retail application. It reads data from a client/retailer data vault and generate all possible alerts for the client based on the new sales data. The alert generation is configurable to generate one or two sets of alerts based on the forecast algorithms available:
* LOESS based forecast (Logistic Regression)
* DRFE Engine

The results from the Alert Generation will be used by the TeamAlert selection component downstream.

For more information:
https://acosta-it.atlassian.net/wiki/spaces/DR/pages/225214800/Info+Mart+-+Alert+Generation
