FILES= histogram_tools.py histogram_specs.json
download: $(FILES)

histogram_tools.py:
	wget -c http://hg.mozilla.org/mozilla-central/raw-file/tip/toolkit/components/telemetry/histogram_tools.py -O $@

histogram_specs.json: tip-histograms.json
	python specgen.py $< > $@

clean:
	rm -f $(FILES)
