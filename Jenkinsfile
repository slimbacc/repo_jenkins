o pipeline {
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
                echo 'python process.py'
            }
        }
        // Add more stages as needed
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
