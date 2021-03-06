#Experiment with Apache Spark

This repo was the result of a takehome exercise for part 1 of an interview process.

## The Assignment
The primary purpose of the assignment is to produce produce a simple data process from user files.
The main goal is to define a new phone marketing campaign for top 1000 users in order to upsell them with a new compelling product.


### The Data
The data is fictitious and has been generated by a program but is representative of something that could have to be done in real life.
We have created the following files:
* The `transactions.txt` file contains information about transactions.
The structure of the file is the following:
Customer ID, transaction amount, transaction date
* The `users.txt` file contains information about users.
The structure of the files is the following:
Customer ID, Customer full name, email list, phone list 
* The `donotcall.txt` file contains phone numbers that should not be used in the campaign


### The Analysis
The selection of the top 1000 users should include the following steps:
* Parsing of the files
* We wish to contact users using phone numbers that are not in the do not call list.
As this is a phone campaign, there should be at least one phone number per user in the output.
* Selecting the top users based on highest transaction amount for year 2015
* Save the campaign in a file using the following structure:
Customer ID, Customer name, phone list that can be used to contact the user, total transaction amount

## My Solution
I did the assignment in both base Python (process.py and output.txt) and using Spark+pyspark (spark_process.py and spark_output.txt).  I wanted to do it this way to verify that my results using Spark matched my base Python output. I was in fact able to get the same results for both!

### Process
* Remove do-not-call (DNC) numbers from each user record.
* Filter out any users who have zero phone numbers associated with them after DNC removal - eligible users will then have at least one phone number.
* Remove transactions that happened before 2015-01-01.
* Sum 2015 transactions, grouped by user. 
* Join transactions to eligible users, sorting in descending order by 2015 transaction total.
* Take top 1000 users.

### To Do

The one thing that I struggled with was how to preserve the customer id and phone number fields when joining my user and transaction data in Spark. For some reason, I couldn't grab those fields - I think it's likely some small join error that I can't see.