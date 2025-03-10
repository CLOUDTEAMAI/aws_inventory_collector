from os import environ
import subprocess

all_keys = {
    "651205910449": {
        "key": "ASIAZPHXFT6YRDKWHQQH",
        "secret": "mBOEGva9shzr/XYs4sORX1BDI//7BaCakWROquJu"
    },
    "207567795677": {
        "key": "ASIATAVABGXO32UUCOA6",
        "secret": "z8BpVPDjIXNwwT48XnaBnS8sorWGFDiquvJ0PdKS"
    },
    "442042518035": {
        "key": "ASIAWN26JRIJ2J4535UJ",
        "secret": "pa36OTzjqpXAhyHSf+mFAtTiY0jUrPvbrDeDx+XG"
    },
    "954976303527": {
        "key": "ASIA54WIF7GTUJZS75FQ",
        "secret": "eTMoPJpNNU+mMb2Bru/c/9nX1oUB+9VY6T1u2/fV"
    },
    "285565604150": {
        "key": "ASIAUE7IJKE3KW6NYJAL",
        "secret": "DUCOOvedkLkUgXWu1OxTRscvFa/+4QELFNJRmZq8"
    },
    "447048465746": {
        "key": "ASIAWQFRKHVJEIS5XIWV",
        "secret": "CcsUqY9QXHI/T3IDSAah6ItFT74oCsVIKR39G+u3"
    },
    "289628097501": {
        "key": "ASIAUG3ZOBPO62EUPDYL",
        "secret": "ErKjrAs2GpAxjR7tYuOhh7EuNlI3ytWdDV3cgKml"
    },
    "712023455730": {
        "key": "ASIA2LR7HW7ZLNWOIGTG",
        "secret": "Pae2+Y/73jl7Nsd5gVhhanJszYQHmZF9iNtwVl+r"
    },
    "767397787490": {
        "key": "ASIA3FLDY65RFII3B54H",
        "secret": "RusSt+e9RCLTc2TESAl1koXWHRu+MgLRsKtPkRgl"
    },
    "937662483182": {
        "key": "ASIA5UUIKBLXPAGABC4I",
        "secret": "TvETf4e3idcIo7S3o/Qb4EKItZuqAlMGOFytzV9Y"
    },
    "532565123595": {
        "key": "ASIAXX7224IFTINUIJD5",
        "secret": "OFDgU6+mHwuLJ2BnayHs+g8ywgvOEjbFcZVqsVQu"
    },
    "802216910313": {
        "key": "ASIA3VR62TXUXNFMK3PA",
        "secret": "CJTRJYmCNjvxFFPMXfSkA32rEPTq1RordWdedaaY"
    },
    "057348804435": {
        "key": "ASIAQ2WSBJ5J3LX4UAR6",
        "secret": "HBtY9WN6K0oTLpRMYR4HDwodRnJnNPlXnLoZEUFb"
    },
    "765201379690": {
        "key": "ASIA3EKMOOFVPYCBBIIZ",
        "secret": "6mstYMbiqNxdyuMB/x7oZPnw3tWn6KZORcHv2SZC"
    },
    "124355677960": {
        "key": "ASIARZ5BNB4EOEYWZGMH",
        "secret": "3TksDS36bAoLxlbSvpZ22SyXWhWedwTq+NOFo545"
    },
    "940101027570": {
        "key": "ASIA5VYTDI3ZPVEYEZXS",
        "secret": "W1XS6nv9CXwFf9iqlh0Uf5iqdeTSrA5usGDscRBq"
    },
    "831926610422": {
        "key": "ASIA4DMVQVH3BONB2YQ5",
        "secret": "K+1MvOB+txUUq/LJ6WmcTUG6SoNEvNpyM7hyp+DC"
    },
    "590183694221": {
        "key": "ASIAYS2NQP6GWWSFP5EH",
        "secret": "CU3YY+1GIYYIOiLGfGQJWqifI2x/b7EETuTuFoCk"
    },
    "594281923127": {
        "key": "ASIAYUXPWQI3TTDM2BST",
        "secret": "rvJduzuFDMCW7EMaDKY6Ts2qVXtH50KzmJGP6iNi"
    },
    "706802301927": {
        "key": "ASIA2JEFTHPTS3RWOESP",
        "secret": "kVav3E+ewukrg+YEpvR+6+MuRsSO4WA/jfvWdlcW"
    },
    "736794117462": {
        "key": "ASIA2XDC2AVLJ7DFLARX",
        "secret": "Z8ZK0yqDeZrS2xO0tYG5/zSm7IPeAPU6TEOe89ds"
    },
    "637423174500": {
        "key": "ASIAZI2LBYNSGA45M46X",
        "secret": "GNBr2hyHrXX00lRgtb9osfBjZv5MmNgqvBd4vxMb"
    },
}

for k, v in all_keys.items():
    template = '''
{
    "accounts": [
        {
            "account_id": "%s"
        }
    ]
}
''' % k
    with open("/Users/haimdagan/Desktop/Repos/cloudteamai/aws/aws_inventory_collector/files/accounts.json", "w") as file:
        file.write(template)

    print(k)
    print(v)
    env = environ.copy()  # Copy existing environment variables
    env["MY_ENV_VAR"] = "temporary_value"
    environ["AWS_ACCESS_KEY_ID"] = v["key"]
    environ["AWS_SECRET_ACCESS_KEY"] = v["secret"]
    subprocess.run(
        ["python3.11", "/Users/haimdagan/Desktop/Repos/cloudteamai/aws/aws_inventory_collector/main-manual.py"], env=env)
