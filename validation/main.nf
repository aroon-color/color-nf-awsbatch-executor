process taskA {
    cpus 1
    memory '1 GB'
    output:
    path 'result.txt'
    script:
    """
    echo A > result.txt
    """
}

process taskB {
    cpus 1
    memory '1 GB'
    output:
    path 'result.txt'
    script:
    """
    echo B > result.txt
    """
}

workflow {
    taskA()
    taskB()
}
