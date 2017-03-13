module.exports = {
    "parserOptions": {
        "ecmaVersion": 7,
        "sourceType": "module",
        "allowImportExportEverywhere": false,
        "ecmaFeatures": {
            "modules": true
        }
    },
    "rules": {
        "indent": [
            2,
            4
        ],
        "quotes": [
            2,
            "single"
        ],
        "linebreak-style": [
            2,
            "unix"
        ],
        "semi": [
            2,
            "always"
        ],
        "no-console": 0,
        "no-empty": 0,
        "no-constant-condition":0,
        "no-extra-boolean-cast":0,
        "no-extra-bind": 2,
        "no-debugger": 2,
        "no-dupe-keys": 2,
        "no-eval": 2,
        "no-alert": 0,
        "no-redeclare": 2,
        "no-unused-vars": [
            0,
            {
                "vars": "all",
                "args": "none",
                "varsIgnorePattern": "[React|Redux|Link]"
            }
        ],
        "no-unreachable": 2,
        "no-use-before-define": [
            0,
            "nofunc"
        ],
        "handle-callback-err": [
            0,
            "^(err|error)$"
        ],
    },
    "plugins": [
    ],
    "env": {
        "es6": true,
        "node": true,
        "browser": true,
        "jest": true
    },
    "extends": "eslint:recommended"
};
