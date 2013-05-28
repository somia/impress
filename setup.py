import distutils.core

distutils.core.setup(
	name = "impress",
	version = "4",
	packages = [
		"impress",
		"impress.models",
		"impress.patterns",
		"impress_thrift",
		"pycassa",
		"cassandra",
	],
	package_dir = {
		"impress":        "impress",
		"impress_thrift": "gen-py/impress_thrift",
		"pycassa":        "pycassa/pycassa",
		"cassandra":      "pycassa/cassandra",
	},
)
