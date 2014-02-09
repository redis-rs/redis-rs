RUSTC?=rustc
RUSTFLAGS?=
OUTPUT_PATH?=build

CRATE_LIBFILE=src/redis/lib.rs
CRATE_NAME=$(shell $(RUSTC) --crate-name $(CRATE_LIBFILE))
CRATE_FILENAME=$(shell $(RUSTC) --crate-file-name $(CRATE_LIBFILE))
CRATE_DEPS=$(OUTPUT_PATH)/.$(CRATE_FILENAME).deps.mk
CRATE=$(OUTPUT_PATH)/$(CRATE_FILENAME)

CRATE_TESTFILE=src/redis/test.rs
CRATE_TESTRUNNER=$(OUTPUT_PATH)/$(CRATE_NAME)-test
CRATE_TESTRUNNER_DEPS=$(OUTPUT_PATH)/.$(CRATE_NAME)-test.deps.mk

# Convenience functions
all: compile

compile: $(CRATE)

test: $(CRATE_TESTRUNNER)
	@RUST_TEST_TASKS=1 ./$(CRATE_TESTRUNNER)

clean:
	@rm -f $(CRATE_DEPS)
	@rm -f $(CRATE_TESTRUNNER_DEPS)
	@rm -f $(CRATE)
	@rm -f $(CRATE_TESTRUNNER)

.PHONY: all compile test clean

# Build steps
$(CRATE): $(CRATE_LIBFILE)
	@mkdir -p $(OUTPUT_PATH)
	@$(RUSTC) $(RUSTFLAGS) \
		--dep-info=$(CRATE_DEPS) \
		-o $(CRATE) \
		$(CRATE_LIBFILE)

$(CRATE_TESTRUNNER): $(CRATE_TESTFILE) $(CRATE)
	@mkdir -p $(OUTPUT_PATH)
	@$(RUSTC) $(RUSTFLAGS) \
		--dep-info=$(CRATE_TESTRUNNER_DEPS) \
		-o $(CRATE_TESTRUNNER) \
		-L $(OUTPUT_PATH) \
		--test $(CRATE_TESTFILE)

# Dependencies
-include $(CRATE_DEPS)
-include $(CRATE_TESTRUNNER_DEPS)
