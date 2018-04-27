class market_tick(object):
	@property
	def open(this):
		return this._open

	@open.setter
	def open(this, value):
		this._open = value

	@property
	def close(this):
		return this._close

	@close.setter
	def close(this, value):
		this._close = value

	@property
	def high(this):
		return this._high

	@high.setter
	def high(this, value):
		this._high = value

	@property
	def low(this):
		return this._low

	@low.setter
	def low(this, value):
		this._low = value

	@property
	def volume(this):
		return this._volume

	@volume.setter
	def volume(this, value):
		this._volume = value

	@property
	def timestamp(this):
		return this._timestamp

	@timestamp.setter
	def timestamp(this, value):
		this._timestamp = value

	@property
	def time(this):
		return this._time

	@time.setter
	def time(this, value):
		this._time = value

	@property
	def amount(this):
		return this._amount

	@amount.setter
	def amount(this, value):
		this._amount = value

	@property
	def count(this):
		return this._count

	@count.setter
	def count(this, value):
		this._count = value

	@property
	def period(this):
		return this._period

	@period.setter
	def period(this, value):
		this._period = value

