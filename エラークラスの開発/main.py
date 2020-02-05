from exceptions import ClientError

def main():
    try:
        error_response = {
            'Error': {
                'Code': '001',
                'Message': 'raise Error.'
            }
        }
        raise ClientError(error_response, "test")
    except ClientError as e:
        print(e)

if __name__ == "__main__":
    main()
