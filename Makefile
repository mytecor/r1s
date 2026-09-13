.PHONY: check docs-check

check: docs-check

docs-check:
	lychee --no-progress --offline --include-fragments=full .
