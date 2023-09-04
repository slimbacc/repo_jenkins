pipeline {
    agent any
    
    stages {
        stage("Clone Git Repository") {
            steps {
                git(
                    url: "https://github.com/slimbacc/repo_jenkins.git",
                    branch: "main",
                    changelog: true,
                    poll: true
                )
            }
        }
        stage('Build') {
            steps {
                // Run the Python script
                sh 'python process.py'
            }
        }
        stage('Send Build Logs to Kafka') {
            steps {
                sh 'python send_to_kafka.py'
            }
        }
    }
    
    post {
        success {
            echo 'success'
        }
        failure {
            echo 'failure'
        }
    }
}
