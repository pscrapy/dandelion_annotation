# dandelion_annotation
Utility wrapper in python for [Dandelion](https://dandelion.eu/) API calls to implement request throttling in order to respect user quotas.

The wrapper is designed to be left running unattended annotating a collection of texts, sleeping when meeting daily quotas with optional checkpointing.
Authentication data is read from a config.json file containing the user token.
