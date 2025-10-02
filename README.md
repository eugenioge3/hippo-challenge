# Instructions

## Business Model
We work to offer the lowest possible prices on generic and branded medications, regardless of whether you have insurance or not. Patients get prescriptions with drug (`ndc`) and its (`quantity`) to be filled by a Pharmacy (`npi`). When the patient arrives at the pharmacy with the prescription, the pharmacist informs the (`price`) and submits a new claim. Sometimes, the consumer does not return to the pharmacy to get the drugs, this will generate a claim revert which should be registered by the pharmacist in order to revert (i.e. invalidate) that claim.
### Input
#### Arguments
Your application should accept 3 lists of directories names with pharmacy dataset, claims events and reverts events.

- we don't change pharmacy very often;
- claims and reverts are a stream of events.
#### Pharmacy schema
- id (string):
- chain (string):
#### Claims event schema
- id (string): a UUID that identifies the claim.
- npi (string): an identifier of the pharmacy that filled the claim;
- ndc (string): an identifier of the drug;
- price (float): the total price charged for the prescription (i.e. unit_price * quantity);
- quantity (integer): the amount of drugs that was filled by the pharmacy;
- timestamp (datetime): when the claim was filled.
#### Revert event schema
- id (string): a UUID that identifies the revert.
- claim_id (string): an identifier of the claim been reverted (i.e. invalidated);
- timestamp (datetime): when the claim was reverted.
  
Example data can be found on [claims.json](https://gist.github.com/matheus-hellohippo/a0e28bcbeade9e5044a08808a847a11c/raw/02-claims.json).

## Goals
## 1. Read data stored in JSON files
Read pharmacy, claims and reverts from the provided files in your entry point. Some events may not comply with the provided schema. You can use the library of your choice to perform the JSON parsing. We are only interested in events from Pharmacy dataset.

## 2. Calculate metrics for some dimensions
We want to check how some metrics perform depending on a few dimensions. For example, we would like to check the average unit price offered by pharmacies. This will help us to spot new opportunities or pharmacies that are performing poorly.

Metrics:
- Count of claims
- Count of reverts
- Average unit price
- Total price

Dimensions:
- npi
- ndc

Please, write the output to a JSON file using the following format:
```json
[
    {
        "npi": "0000000000",
        "ndc": "00002323401",
        "fills": 82,
        "reverted": 4,
        "avg_price": 377.56,
        "total_price": 2509345.2
    },
    ...
]
```
## 3. Make a recommendation for the top 2 Chain to be displayed for each Drug.
The business team wants to understand Drug unit prices per Chain. To measure performance, we will check the chain that, on average, charges less per drug unit.
Output fields:
Please, write the output to a JSON file using the following format:
```json
[
    {
        "ndc": "00015066812",
        "chain": [
            {
                "name": "health",
                "avg_price": 377.56
            },
            {
                "name": "saint",
                "avg_price": 413.40
            }
        ]
    },
    ...
]
```
## 4. Understand Most common quantity prescribed for a given Drug
The business team wants to know what is the Drug most common quantity prescribed to negotiate prices discounts.
Please, write the output to a JSON file using the following format:
```json
[
    {
        "ndc": "00002323401",
        "most_prescribed_quantity": [
            8.5, 15.0, 45.0, 180.0, 2.0
        ]
    },
    ...
]
```

## Technical requirements
- Write your application using the Scala or Python programming languages. You can choose the build tool of your choice;
- You can use a library of your choice to parse JSON and program arguments;
- Please don't use notebook software (Jupyter, Zeppelin, etc) for this;
- Please, don't use any data processing framework (Spark, Flink, Akka...) for goals 1 and 2. You can use them for the 3 and 4;
- Your application will be running on a single instance with 10 cores;
Please, provide your code as a git repository with a README on how to execute it with the sample files.

## Questions?
If you have clarifying questions, don't hesitate to ask by email.