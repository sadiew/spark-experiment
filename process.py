import codecs


def load_users():
    """Load cities into database."""
    users = {}

    with codecs.open('./users.txt', encoding='utf-8') as source_file:
        source_file.readline()
        for line in source_file:
            user_data = line.rstrip().split(';')
            users[user_data[0]] = {'name': user_data[1],
                                   'emails': user_data[2].split(','),
                                   'nums': user_data[3].split(',')}

    return users


def load_transactions():
    transactions = {}
    with codecs.open('./transactions.txt', encoding='utf-8') as source_file:
        source_file.readline()
        for line in source_file:
            transaction_data = line.rstrip().split(';')
            user_id = transaction_data[0]
            if transaction_data[2] >= '2015-01-01':
                transactions[user_id] = transactions.get(user_id, 0) + float(transaction_data[1][1:])

    return transactions


def load_dnc():
    with codecs.open('./donotcall.txt', encoding='utf-8') as source_file:
        dnc_set = set()
        source_file.readline()
        for line in source_file:
            phone_number = line.rstrip()
            dnc_set.add(phone_number)

    return dnc_set


def remove_dnc_from_users(users, dnc_set):
    for user_id in users:
        for num in users[user_id]['nums']:
            if num in dnc_set or num == '':
                users[user_id]['nums'].remove(num)

    eligible_users = {}
    for user_id in users:
        if users[user_id]['nums']:
            eligible_users[user_id] = users[user_id]

    return eligible_users


def users_to_transactions(eligible_users, transactions):
    joined_list = []
    for user_id in transactions:
        if user_id in eligible_users:
            joined_list.append((user_id,
                                eligible_users[user_id]['name'],
                                eligible_users[user_id]['nums'],
                                eligible_users[user_id]['emails'],
                                round(transactions[user_id], 2)))

    sorted_transactions = sorted(joined_list, key=lambda x: x[4], reverse=True)
    return sorted_transactions[:1000]


def prepare_output_file(top_1000_users):
    output_file = open('output.txt', 'w')
    for user in top_1000_users:
        line = ''
        for item in user:
            line = line + str(item) + '\t'
        line = line.rstrip() + '\n'
        output_file.write(line)
    output_file.close()


if __name__ == "__main__":
    dnc_set = load_dnc()
    users = load_users()
    eligible_users = remove_dnc_from_users(users, dnc_set)
    transactions = load_transactions()
    sorted_transactions = users_to_transactions(eligible_users, transactions)
    prepare_output_file(sorted_transactions)