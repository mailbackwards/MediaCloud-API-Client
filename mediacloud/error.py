class MCException(Exception):
    def __init__(self, message, status_code=0):
        Exception.__init__(self, message)
        self.status_code = status_code

class CustomMCException(MCException):
	def __init__(self, message, status_code=0, mc_resp=None):
		MCException.__init__(self, message, status_code)
		self.mc_resp = mc_resp