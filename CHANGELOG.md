# Changelog

All notable changes to this project will be documented in this file.

## [0.0.2] - Unreleased

### Added
- Additional config parameters.
- Filter for selected years and components.
- Formatting of input data for adjustment.
- Test coverage report.
- Apportion over adjusted negative values.
- GDHIDAP-58: aggregation from LAU to LAD level.
- GDHIDAP-59: negative value apportionment after adjustment.
- GDHIDAP-70: GitHub open repository security and guidance.

### Changed
- Adjustment values now determined by interpolation/ extrapolation.
- Vectorised applying adjustment instead of for loop.

### Deprecated

### Fixed

### Removed
- Scaling factor and headroom adjustment value calculations.

## [0.0.1] - 2025-10-16

### Added
- Full pipeline setup with basic functionality to run a preprocess module
to flag LSOAs to review, and an adjustment module to impute values for LSOAs
marked to be adjusted.
