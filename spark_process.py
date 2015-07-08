from pyspark import SparkContext


def make_dnc_list(input_file):
    dnc_data = sc.textFile(input_file)
    dnc_records = dnc_data.count()
    return dnc_data.take(dnc_records)


def remove_dnc_nums(line):
    nums = line[3].split(',')
    for num in nums:
        if num in dnc_list or num == '':
            nums.remove(num)
    line[3] = nums

    return line


def process_users(input_file):
    users_with_nums = sc.textFile(input_file) \
                    .map(lambda line: line.split(';')) \
                    .map(remove_dnc_nums) \
                    .filter(lambda line: len(line[3])>0)
    return users_with_nums


def process_transactions(input_file):
    transactions_2015 = sc.textFile(input_file) \
                        .map(lambda line: line.split(';')) \
                        .filter(lambda line: line[2] >= '2015-01-01') \
                        .map(lambda line: line[:2]) \
                        .map(lambda line: [line[0], float(line[1][1:])]) \
                        .reduceByKey(lambda a, b: a + b)
    return transactions_2015


def join_transactions_to_users(users_with_nums, transactions_2015):
    trans_joined_to_users = transactions_2015 \
                        .join(users_with_nums) \
                        .map(lambda a: a[1]).map(lambda a: (a[0], a[1])) \
                        .sortByKey(ascending=False)
    return trans_joined_to_users.take(1000)

def prepare_output_file(top_1000_users):
    output_file = open('RadiusDataEng/spark_output.txt', 'a')
    for user in top_1000_users:
        line = user[1] + '\t' + str(user[0]) + '\n'
        output_file.write(line)
        print line
    output_file.close()


if __name__ == '__main__':
    sc = SparkContext("local", "Radius Example")
    dnc_list = make_dnc_list("RadiusDataEng/donotcall.txt")
    users_with_nums = process_users("RadiusDataEng/users.txt")
    transactions_2015 = process_transactions("RadiusDataEng/transactions.txt")
    trans_joined_to_users = join_transactions_to_users(users_with_nums, transactions_2015)
    prepare_output_file(trans_joined_to_users)











