from .fyersService import FyersService
from .fyersService import FyersAsyncService
import json
from .config import Config
import subprocess
import sys
#service = FyersService()

class FyersModel:
	def __init__(self,is_async=False):
		if is_async is False:
			self.service = FyersService()
		else:
			self.service = FyersAsyncService()

	def get_profile(self,token):
		response = self.service.getCall(Config.get_profile,token)		
		return response

	def tradebook(self,token):
		response = self.service.getCall(Config.tradebook,token)		
		return response

	def positions(self,token):
		response = self.service.getCall(Config.positions,token)
		return response

	def holdings(self,token):
		response = self.service.getCall(Config.holdings,token)
		return response

	def convert_position(self,token,data):
		response = self.service.postCall(Config.convertPosition,token,data)
		return response

	def funds(self,token):
		response = self.service.getCall(Config.funds,token)
		return response

	def orders(self,token):
		response = self.service.getCall(Config.orders,token)
		return response		

	def delete_orders(self,token,data):
		response = self.service.deleteCall(Config.orders,token,data)
		return response		

	def place_orders(self,token,data):
		response = self.service.postCall(Config.orders,token,data)
		return response

	def update_orders(self,token,data):
		response = self.service.putCall(Config.orders,token,data)
		return response

	def minquantity(self,token):
		response = self.service.getCall(Config.minquantity,token)
		return response

	def order_status(self,token,data):
		response = self.service.getCall(Config.orderStatus,token,data)
		return response

	def market_status(self,token):
		response = self.service.getCall(Config.marketStatus,token)
		return response	

	def exit_positions(self,token,data=None):
		response = self.service.postCall(Config.exitPositions,token,data)
		return response

	def generate_data_token(self,token,data):
		allPackages = subprocess.check_output([sys.executable, '-m', 'pip', 'freeze'])
		installed_packages = [r.decode().split('==')[0] for r in allPackages.split()]
		if Config.dataVendorTD not in installed_packages:
			print("Please install truedata package | pip install truedata-ws")
		response = self.service.postCall(Config.generateDataToken,token,data)
		return response
