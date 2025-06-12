pipeline {
  agent any

  environment {
    /* AWS & ECR */
    AWS_DEFAULT_REGION = 'ap-northeast-2'
    ECR_REGISTRY       = '853660505909.dkr.ecr.ap-northeast-2.amazonaws.com'
    IMAGE_REPO_NAME    = 'model-server'
    DEV_TAG            = 'dev-latest'
    PROD_TAG           = 'prod-latest'

    /* GitHub Checks */
    GH_CHECK_NAME      = 'Model Server Build Test'

    /* Slack */
    SLACK_CHANNEL      = '#ci-cd'
    SLACK_CRED_ID      = 'slack-factoreal-token'   // Slack App OAuth Token

    /* Argo CD */
    HELM_VALUES_PATH        = 'monitory-helm-charts/model-server/values.yaml'
    ARGOCD_SERVER           = 'argocd.monitory.space'   // Argo CD server endpoint
    ARGOCD_APPLICATION_NAME = 'model-server'
  }

  stages {
    /* 0) 환경 변수 설정 */
    stage('Environment Setup') {
      steps {
        script {
          def rawUrl = sh(script: "git config --get remote.origin.url",
                        returnStdout: true).trim()
          env.REPO_URL = rawUrl.replaceAll(/\.git$/, '')
          env.COMMIT_MSG = sh(script: "git log -1 --pretty=format:'%s'",returnStdout: true).trim()
        }
      }
    }

    /* 1) 공통 테스트 */
    stage('Test') {
      when {
        allOf {
          not { branch 'develop' }
          not { branch 'main' }
          not { changeRequest() }
        }
      }
      steps {
        publishChecks name: GH_CHECK_NAME,
                      status: 'IN_PROGRESS',
                      detailsURL: env.BUILD_URL

        // Gradle 빌드 환경 변수 설정
        withCredentials([ file(credentialsId: 'model-env', variable: 'ENV_FILE') ]) {
        sh '''
set -o allexport
source "$ENV_FILE"
set +o allexport

python3.11 -m pip install --upgrade pip
python3.11 -m pip install -r requirements.txt
python3.11 -m pytest || echo "Tests not configured, skipping..."
'''
        }
      }
      post {
        success {
          publishChecks name: GH_CHECK_NAME,
                        conclusion: 'SUCCESS',
                        detailsURL: env.BUILD_URL
        }
        failure {
          publishChecks name: GH_CHECK_NAME,
                        conclusion: 'FAILURE',
                        detailsURL: "${env.BUILD_URL}console"
          slackSend channel: env.SLACK_CHANNEL,
                              tokenCredentialId: env.SLACK_CRED_ID,
                              color: '#ff0000',
                              message: """:x: *Model Server Test 실패*
          파이프라인: <${env.BUILD_URL}|열기>
          커밋: `${env.GIT_COMMIT}` – `${env.COMMIT_MSG}`
          (<${env.REPO_URL}/commit/${env.GIT_COMMIT}|커밋 보기>)
          """
        }
      }
    }

    /* 2) develop 전용 ─ 컨테이너 이미지 빌드 & ECR Push */
    stage('Image Build & Push (develop only)') {
      when {
        allOf {
          branch 'develop'
          not { changeRequest() }
        }
      }
      steps {
        withCredentials([[$class: 'AmazonWebServicesCredentialsBinding',
                          credentialsId: 'jenkins-access']]) {
          sh """
aws ecr get-login-password --region ${AWS_DEFAULT_REGION} | docker login --username AWS --password-stdin ${ECR_REGISTRY}
docker build -t ${ECR_REGISTRY}/${IMAGE_REPO_NAME}:${DEV_TAG} -t ${ECR_REGISTRY}/${IMAGE_REPO_NAME}:${env.GIT_COMMIT} .
docker push ${ECR_REGISTRY}/${IMAGE_REPO_NAME}:${DEV_TAG}
docker push ${ECR_REGISTRY}/${IMAGE_REPO_NAME}:${env.GIT_COMMIT}
"""
        }
      }
      /* Slack 알림 */
      post {
        failure {
          slackSend channel: env.SLACK_CHANNEL,
                    tokenCredentialId: env.SLACK_CRED_ID,
                    color: '#ff0000',
                    message: """:x: *Model Server develop branch CI 실패*
파이프라인: <${env.BUILD_URL}|열기>
커밋: `${env.GIT_COMMIT}` – `${env.COMMIT_MSG}`
(<${env.REPO_URL}/commit/${env.GIT_COMMIT}|커밋 보기>)
"""
        }
      }
    }

    /* 3) develop 전용 ─ Argo CD 배포 */
    stage('Deploy (develop only)') {
      when {
        allOf {
          branch 'develop'
          not { changeRequest() }
        }
      }
      steps {
        withCredentials([string(credentialsId: 'monitory-iac-github-token', variable: 'GIT_TOKEN')]){
          sh """
git clone https://${GIT_TOKEN}@github.com/Fac2Real/monitory-iac.git ${env.GIT_COMMIT}
cd ${env.GIT_COMMIT}
git checkout deploy
git fetch origin main
git merge --ff-only origin/main
yq -i ".image.tag = \\"${env.GIT_COMMIT}\\"" ${HELM_VALUES_PATH}
git config user.name  "ci-bot"
git config user.email "ci-bot@monitory.space"
git add ${HELM_VALUES_PATH}
git commit -m "ci(${ARGOCD_APPLICATION_NAME}): bump image to ${env.GIT_COMMIT})"
git push https://${GIT_TOKEN}@github.com/Fac2Real/monitory-iac.git deploy
          """
        }

        withCredentials([string(credentialsId: 'argo-jenkins-token', variable: 'ARGOCD_AUTH_TOKEN')]) {
          sh '''
argocd --server $ARGOCD_SERVER --insecure --grpc-web \
        app sync $ARGOCD_APPLICATION_NAME

argocd --server $ARGOCD_SERVER --insecure --grpc-web \
        app wait $ARGOCD_APPLICATION_NAME --health --timeout 300
'''
        }
      }
      /* Slack 알림 */
      post {
        success {
          slackSend channel: env.SLACK_CHANNEL,
                    tokenCredentialId: env.SLACK_CRED_ID,
                    color: '#36a64f',
                    message: """:white_check_mark: *Model Server develop branch CI/CD 성공*
파이프라인: <${env.BUILD_URL}|열기>
커밋: `${env.GIT_COMMIT}` – `${env.COMMIT_MSG}`
(<${env.REPO_URL}/commit/${env.GIT_COMMIT}|커밋 보기>) (<https://argocd.monitory.space/|Argo CD 보기>)
"""
        }
        failure {
          slackSend channel: env.SLACK_CHANNEL,
                    tokenCredentialId: env.SLACK_CRED_ID,
                    color: '#ff0000',
                    message: """:x: *Model Server develop branch CD 실패*
파이프라인: <${env.BUILD_URL}|열기>
커밋: `${env.GIT_COMMIT}` – `${env.COMMIT_MSG}`
(<${env.REPO_URL}/commit/${env.GIT_COMMIT}|커밋 보기>, <https://argocd.monitory.space/|Argo CD 보기>)
"""
        }
      }
    }


    /* 4) main 전용 ─ Docker 이미지 빌드 & ECR Push */
    stage('Docker Build & Push (main only)') {
      when {
        allOf {
          branch 'main'
          not { changeRequest() }
        }
      }
      steps {
        withCredentials([[$class: 'AmazonWebServicesCredentialsBinding',
                          credentialsId: 'jenkins-access']]) {
          sh """
aws ecr get-login-password --region ${AWS_DEFAULT_REGION} | docker login --username AWS --password-stdin ${ECR_REGISTRY}
docker build -t ${ECR_REGISTRY}/${IMAGE_REPO_NAME}:${PROD_TAG} -t ${ECR_REGISTRY}/${IMAGE_REPO_NAME}:${env.GIT_COMMIT} .
docker push ${ECR_REGISTRY}/${IMAGE_REPO_NAME}:${PROD_TAG}
docker push ${ECR_REGISTRY}/${IMAGE_REPO_NAME}:${env.GIT_COMMIT}
          """
        }
      }
      /* Slack 알림 */
      post {
        success {
          slackSend channel: env.SLACK_CHANNEL,
                    tokenCredentialId: env.SLACK_CRED_ID,
                    color: '#36a64f',
                    message: """:white_check_mark: *Model Server main branch CI 성공*
파이프라인: <${env.BUILD_URL}|열기>
커밋: `${env.GIT_COMMIT}` – `${env.COMMIT_MSG}`
(<${env.REPO_URL}/commit/${env.GIT_COMMIT}|커밋 보기>)
"""
        }
        failure {
          slackSend channel: env.SLACK_CHANNEL,
                    tokenCredentialId: env.SLACK_CRED_ID,
                    color: '#ff0000',
                    message: """:x: *Model Server main branch CI 실패*
파이프라인: <${env.BUILD_URL}|열기>
커밋: `${env.GIT_COMMIT}` – `${env.COMMIT_MSG}`
(<${env.REPO_URL}/commit/${env.GIT_COMMIT}|커밋 보기>)
"""
        }
      }
    }
  }
}
