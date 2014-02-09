RUSTC?=rustc
RUSTFLAGS?=

BUILD_FOLDER?=build

CRATE_LIBFILE=src/redis/lib.rs
CRATE_NAME=$(shell $(RUSTC) --crate-name $(CRATE_LIBFILE))
CRATE_FILENAME=$(shell $(RUSTC) --crate-file-name $(CRATE_LIBFILE))
CRATE_DEPS=$(BUILD_FOLDER)/.$(CRATE_FILENAME).deps.mk
CRATE=$(BUILD_FOLDER)/$(CRATE_FILENAME)

CRATE_TESTFILE=src/redis/test.rs
CRATE_TESTRUNNER=$(BUILD_FOLDER)/$(CRATE_NAME)-test
CRATE_TESTRUNNER_DEPS=$(BUILD_FOLDER)/.$(CRATE_NAME)-test.deps.mk

# Convenience functions
all: compile

compile: $(CRATE)

test: $(CRATE_TESTRUNNER)
	RUST_TEST_TASKS=1 ./$(CRATE_TESTRUNNER)

clean:
	rm -f $(CRATE_DEPS)
	rm -f $(CRATE)
	rm -f $(CRATE_TESTRUNNER)

.PHONY: all compile test clean

# Build steps
$(BUILD_FOLDER):
	mkdir -p $(BUILD_FOLDER)

$(CRATE_TESTRUNNER): $(CRATE_TESTFILE) $(CRATE)
	$(RUSTC) $(RUSTFLAGS) --dep-info=$(CRATE_TESTRUNNER_DEPS) \
		-L $(BUILD_FOLDER) --test $(CRATE_TESTFILE) \
		-o $(CRATE_TESTRUNNER)

$(CRATE): build $(CRATE_LIBFILE)
	$(RUSTC) $(RUSTFLAGS) --dep-info=$(CRATE_DEPS) $(CRATE_LIBFILE) -o $(CRATE)

# Dependencies
-include $(CRATE_DEPS)
-include $(CRATE_TESTRUNNER_DEPS)
