.. _cartool:

cartool
=======

``cartool`` aims to be a CLI swiss-army-knife for analysing and modifying atproto repos stored inside CAR files.

.. code-block:: text

	USAGE: cartool COMMAND [args...]

	Available commands:
	info <car_path> : print CAR header and repo info
	list <car_path> : list all records in the CAR (values as CIDs)
	dump <car_path> : dump all records in the CAR (values as JSON)
	dump_record <car_path> <key> : dump a single record, keyed on ('collection/rkey')
	compact <car_in> <car_out> : rewrite the whole CAR, dropping any duplicated or unreferenced blocks
	diff <car_a> <car_b> : list the record diff between two CAR files

