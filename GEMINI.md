The following resources must be followed to ensure high quality, 
idiomatic Go code:
1. [Effective Go](https://go.dev/doc/effective_go)
2. [Common Go Patterns for Performance](https://goperf.dev/01-common-patterns/)
3. [Practical Networking Patterns in Go](https://goperf.dev/02-networking/)

Additionally, you must adhere to the following principals:
1. All code must have associated unit tests where practical.
2. All requirements must be validated by integration tests.
3. All code must be SOLID.
4. All code must be clean.
5. Duplication must be eliminated.
5. All modules and structs must "do one thing and do it well". Avoid 
   large modules and functions.
6. All code must be free of concurrency issues.
7. Code quality or requirements must never be sacrificed in order to 
   unblock progress.
8. You must not skip requirements without permission. Given an impossible 
   requirement, provide an analysis of why the requirement is impossible 
   and ask how to move forward.
9. You must maintain professionalism in all code and documentation. You 
   must not include emojis.
10. You must optimize for long term maintenance. The code must be adaptable 
    for new, unknown requirements.
11. You must optimize for easy contribution for human developers. Human 
    developers are your peers, and you are working towards a common goal.

When you are given the requirements, you must execute the following loop:
1. Ask for additional details if any requirements are unclear. Do not make 
   assumptions.
2. Provide a plan for review.
3. Write production code.
4. Compile production code.
5. Correct compilation errors.
4. Write unit tests.
5. Execute unit tests.
6. Correct any failed unit tests, ensuring that all modifications to the 
   unit tests and production code align with requirements and the principals 
   noted above.
7. Write integration tests.
8. Execute integration tests.
9. Correct any failed integration tests, ensuring that all modifications to 
   the project align with requirements and the principals noted above.

If at any point, you are not able to resolve a compilation error or failed 
test:
1. After a single attempt, you must consult the official documentation for 
   the library you are using on https://pkg.go.dev , the project's website 
   and the project's GitHub documentation.
2. After a third attempt, you must summarize the problem for me along with 
   a concise list of what you tried. I will provide you with instructions to 
   proceed.
