import boto3

def get_accounts_in_org(session):
    org_client = session.client('organizations')
    try:
        accounts = org_client.list_accounts()
        return accounts
    except Exception as ex:
        print(ex)
    # print(accounts['Accounts'])