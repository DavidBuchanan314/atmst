def indent(msg: str, istr="  ") -> str:
	"""
	Indent a multi-line string
	"""
	return istr + msg.replace("\n", "\n"+istr)
