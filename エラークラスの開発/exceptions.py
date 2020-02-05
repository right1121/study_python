class ClientError(Exception):
    MSG_TEMPLATE = (
        'An error occurred ({error_code}) when calling the {operation_name} '
        'operation{retry_info}: {error_message}')

    def __init__(self, error_response, operation_name):
        retry_info = self._get_retry_info(error_response)
        error = error_response.get('Error', {})
        msg = self.MSG_TEMPLATE.format(
            error_code=error.get('Code', 'Unknown'),
            error_message=error.get('Message', 'Unknown'),
            operation_name=operation_name,
            retry_info=retry_info,
        )
        super(ClientError, self).__init__(msg)
        self.response = error_response
        self.operation_name = operation_name

    def _get_retry_info(self, response):
        retry_info = ''
        if 'ResponseMetadata' in response:
            metadata = response['ResponseMetadata']
            if metadata.get('MaxAttemptsReached', False):
                if 'RetryAttempts' in metadata:
                    retry_info = (' (reached max retries: %s)' %
                                  metadata['RetryAttempts'])
        return retry_info
