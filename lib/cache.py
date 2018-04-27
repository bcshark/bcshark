import mmap
import os
import sys
import os
import sys
import struct

class cache_manager_factory(object):
	@staticmethod
	def create(cache_settings):
		if cache_settings['enabled'] and cache_settings.has_key('shared_memory'):
			return cache_manager(cache_settings['shared_memory'])

		return None

class cache_manager(object):
	DATA_FORMAT = "lffff"
	DATA_LENGTH = 24

	def __init__(this, settings):
		this.settings = settings
		this.mapping_collection = {}
		
	def dispose(this):
		for market_name, item in this.mapping_collection.items():
			item[0].close()
			item[1].close()

	def get_market_file_path(this, market_name):
		if this.settings.has_key('abspath'):
			data_files_path = this.settings['abspath']
		elif this.settings.has_key('path'):
			shared_memory_path = this.settings['path']
			data_files_path = os.path.normpath(os.path.join(sys.path[0], shared_memory_path))
		market_file_path = os.path.join(data_files_path, market_name)
		return market_file_path

	def get_mapping(this, market_name, symbol_name):
		if this.mapping_collection.has_key(market_name):
			return this.mapping_collection[market_name]
		
		# NOTE: not implement for multiple symbols
		if symbol_name in this.settings['symbols']:
			market_file_path = this.get_market_file_path(market_name)

			if not os.path.exists(market_file_path):
				with open(market_file_path, "wb") as f:
					f.write(struct.pack(this.DATA_FORMAT, 0, 0, 0, 0, 0))

			file_obj = open(market_file_path, "r+b")
			file_obj.seek(0)
			print file_obj.fileno()
			mmap_obj = mmap.mmap(file_obj.fileno(), this.DATA_LENGTH, mmap.MAP_SHARED)

			this.mapping_collection[market_name] = (mmap_obj, file_obj)

			return mmap_obj, file_obj
		else:
			return None, None

	def save_market_symbol_tick(this, market_name, symbol_name, tick):
		mmap_obj, file_obj = this.get_mapping(market_name, symbol_name)

		if mmap_obj:
			mmap_obj.seek(0)
			mmap_obj.write(struct.pack(this.DATA_FORMAT, tick[0], tick[1], tick[2], tick[3], tick[4]))

	def load_market_symbol_tick(this, market_name, symbol_name):
		if not os.path.exists(this.get_market_file_path(market_name)):
			return None

		mmap_obj, file_obj = this.get_mapping(market_name, symbol_name)

		if mmap_obj:
			mmap_obj.seek(0)
			data_obj = struct.unpack(this.DATA_FORMAT, mmap_obj.read(this.DATA_LENGTH))
			return data_obj

		return None