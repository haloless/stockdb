


# Copilot Instructions for Development

## General Guidelines
- Follow PEP8 style guide for all Python code.
- Use descriptive variable and function names.
- Write docstrings for all public functions and classes.
- Prefer list comprehensions and generator expressions where appropriate.
- Avoid global variables unless necessary.

## Dev env

On windows - dev env should source activate script `%USERPROFILE%\Anaconda3\Scripts\activate.bat`
Do not try to alter the env.
All functionality and dependency should be available in above Conda env.
Otherwise user may have difficulty to run the project.


## Testing
- Write unit tests using `pytest` or `unittest`.
- Use Flask's test client for endpoint testing.
- Mock external dependencies in tests.

## Documentation
- Document API endpoints with OpenAPI/Swagger if applicable.
- Maintain a `README.md` with setup and usage instructions.

## Security
- Never commit secrets or credentials to version control.
- Use Flask's built-in protections against CSRF and XSS.
- Regularly update dependencies to patch vulnerabilities.


