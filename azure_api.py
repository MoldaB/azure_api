import logging
import os
from typing import List
import requests

MAX_AZURE_API_RETRIES = 3
INVOKE_REST_API_TIMEOUT_SECONDS = 30
AZURE_API_DOWNLOAD_TIMEOUT_SECONDS = 600


class AzureAPI():
    def __init__(self, organization, project, log=logging, retries=MAX_AZURE_API_RETRIES, , headers={}):
        self.base_url = f"https://dev.azure.com/{organization}/{project}/_apis"
        self.base_params = {"api-version": "5.1"}
        self.headers = { 'Content-Type': 'application/json' }
        self.log = log
        self.retries = retries

    def get_artifact_for_repo_and_branch(self,
                                         artifact_name: str,
                                         repository: str,
                                         branch: str = 'master',
                                         is_pr: bool = False,
                                         build_version_prefix: str = "*",
                                         top_n_builds_to_check=300):
        """

        :param is_pr: flag for is artifact of pr
        :param artifact_name: name of artifact, built from format described in VersionsConfigurations.ini
        :param repository: repo id (guid) of the required artifact
        :param branch: branch name of the required artifact
        :param build_version_prefix: prefix for build number
        :param top_n_builds_to_check: range of builds to search artifacts for
        :return: artifact download link
        """
        repo_response_dict = self.get_repository_by_name(repository)
        repo_id = repo_response_dict['id']
        # build branch name for pr
        if is_pr:
            pr = self.get_pull_request_id_by_branch(repo_id, branch)
            branch = f"refs/pull/{pr['pullRequestId']}/merge"
            build_version_prefix = ""
        top_builds = self.get_top_n_builds_for_repo_and_branch(repo_id, branch, is_pr, top_n_builds_to_check,
                                                               build_version_prefix or "")
        for build in top_builds:
            artifact = self.get_artifact_details(build['id'], artifact_name)
            if artifact:
                return build['buildNumber'].split('-')[0], artifact['resource']['downloadUrl']
        return None, None

    def get_repository_by_name(self, name):
        if not name:
            raise ValueError(f"Cannot get repository by name for - {name}")
        return self._run_get_request(f"git/repositories/{name}")

    def get_pull_request_id_by_branch(self, repo_id: str, branch: str):
        if not branch or branch == 'master':
            raise ValueError(f"invalid pr with source branch of '{branch}'")
        res = self._run_get_request(f"git/repositories/{repo_id}/pullrequests", {
            "searchCriteria.status": "active",
            "searchCriteria.sourceRefName": f"refs/heads/{branch}",
            "$top": 1
        })
        if res['value']:
            return res['value'][0]
        else:
            return None

    def get_top_n_builds_for_repo_and_branch(self, repo_id: str, branch: str = "master", is_pr: bool = False,
                                             n: int = 1,
                                             build_number: str = ""):
        if not repo_id:
            raise ValueError(f"Invalid repo id - {repo_id}")

        reason_filter = 'pullRequest' if is_pr else 'batchedCI,manual,individualCI'
        status_filter = 'active' if is_pr else 'completed'
        branch_name = branch if is_pr else f"refs/heads/{branch}"
        result_filter = '' if is_pr else 'succeeded,partiallySucceeded'

        builds = self._run_get_request("build/builds", params={
            "reasonFilter": reason_filter,
            "statusFilter": status_filter,
            "resultFilter": result_filter,
            "$top": str(n),
            "branchName": branch_name,
            "repositoryId": repo_id,
            "repositoryType": "TfsGit",
            "queryOrder": "finishTimeDescending",
            "buildNumber": f"{build_number}*"
        })
        if not builds.get('value'):
            raise Exception(f"No builds for given parameters | branch: {branch} | repo: {repo_id}")
        return builds['value']

    def get_artifact_details(self, build_id: str, artifact_name: str):
        if not build_id:
            raise ValueError(f"Invalid build id - {build_id}")
        if not artifact_name:
            raise ValueError(f"Invalid artifact name - {artifact_name}")
        return self._run_get_request(f"build/builds/{build_id}/artifacts", params={'artifactName': artifact_name})

    def get_branch_by_name(self, repo_id: str, branch_name: str):
        if not branch_name:
            raise ValueError("Branch name cannot be empty")
        return self._run_get_request(f"git/repositories/{repo_id}/refs", params={'filter': f'heads/{branch_name}'})

    def read_file_from_repo(self, repo_id: str, path_to_file: str, branch_name: str = ''):
        if not path_to_file:
            raise ValueError(f"File path cannot be empty")
        return self._run_get_request(f"git/repositories/{repo_id}/items", params={'path': path_to_file,
                                                                                  'versionDescriptor.version': branch_name})

    def create_new_branch(self, repo_id: str, data: dict):
        return self._run_post_request(f"git/repositories/{repo_id}/pushes", data=data)

    def create_pr(self, repo_id: str, source_branch_name: str, new_branch_name: str, title: str, description: str,
                  is_draft: bool = False):
        data = {"sourceRefName": source_branch_name,
                "targetRefName": new_branch_name,
                "title": title,
                "description": description,
                "isDraft": str(is_draft)}
        return self._run_post_request(f"git/repositories/{repo_id}/pullrequests", data=data)

    def abandon_pr(self, repo_id, pr_id):
        params = self.base_params.copy()
        params['api-version'] = '6.1-preview.1'
        data = {"status": 'abandoned'}
        return self._run_patch_request(f"git/repositories/{repo_id}/pullrequests/{pr_id}", data=data, params=params)

    def delete_branch(self, repo_id, branch_name, branch_id):
        data = [{
            "name": branch_name,
            "oldObjectId": branch_id,
            "newObjectId": "0000000000000000000000000000000000000000"}]
        return self._run_post_request(f"git/repositories/{repo_id}/refs", data=data)

    def queue_build(self, data: dict):
        params = self.base_params.copy()
        params['api-version'] = '6.0'
        return self._run_post_request(f"build/builds", data=data, params=params)

    def get_build_by_id(self, definition_id):
        return self._run_get_request(f"build/builds/{definition_id}")

    def get_definition_by_name(self, name: str):
        if not name:
            raise ValueError(f"Definition name cannot be empty")
        return self._run_get_request(f"build/definitions", params={'name': name})

    @staticmethod
    def download_artifact(url: str, download_path: str, log=None, retries: int = 3):
        res = None
        retry = 0
        while retry < retries:
            retry += 1
            if log and retry > 1:
                log.info(AzureAPI, f"Retry download artifact #{retry}")
            res = requests.get(url,
                               headers=self.headers,
                               timeout=AZURE_API_DOWNLOAD_TIMEOUT_SECONDS,
                               allow_redirects=True)
            if res.status_code == definitions.AZURE_GET_SUCCESS_CODE:
                break
        if not res:
            raise TimeoutError("Failed to download artifact")
        if res.status_code != definitions.AZURE_GET_SUCCESS_CODE:
            raise Exception(res.text)
        zip_path = os.path.join(download_path, "artifact.zip")
        open(zip_path, 'wb').write(res.content)
        return zip_path

    def get_repos_contains_branch(self, relevant_repos: List[str], branch_name: str) -> List[str]:
        """
        Returns all repo names that has an existing branch name.
        Args:
            relevant_repos: All repo names to check.
            branch_name: Wanted branch name.

        Returns:
            List of repo names.
        """
        repos_with_branch_name = []
        for supported_repo in relevant_repos:
            response = self.get_branch_by_name(supported_repo, branch_name)
            if response is not None and response['count'] > 0:
                repos_with_branch_name.append(supported_repo)
        return repos_with_branch_name
     
     def __run_request(self, request_type: requests.api, url: str, as_json: bool = True, params: dict = None,
                      data: dict = None):
        request_args = {'url': f"{self.base_url}/{url}",
                        'headers': self.base_headers,
                        'timeout': INVOKE_REST_API_TIMEOUT_SECONDS,
                        'params': {**self.base_params, **(params or {})}}

        if data:
            request_args['data'] = jsons.dumps(data)
        retry = 0
        while retry < self.retries:
            try:
                res = request_type(**request_args)
                # Verify request succeeded
                if res.status_code == requests.codes.ok or res.status_code == requests.codes.created:
                    if as_json:
                        return jsons.loads(res.content.decode('utf-8'))
                    else:
                        return res.content
                elif res.status_code >= HTTPResponseCode.INTERNAL_SERVER_ERROR:
                    res.raise_for_status()
                else:
                    self.log.warn(f"Azure API responded with non OK status code | "
                                  f"code - {res.status_code} | url - {url} | params - {params}")
                    return None
            except Exception as e:
                self.log.info(f"Exception occurred while trying to reach the server | {e}")
                retry += 1
                self.log.debug(f"Retry invoke REST-API from Azure url: {url}")
                time.sleep(2 ** (retry + 1))

    def _run_get_request(self, url, params: dict = None, as_json: bool = True):
        return self.__run_request(requests.get, url, as_json, params=params)

    def _run_post_request(self, url, params: dict = None, data=None, as_json: bool = True):
        return self.__run_request(requests.post, url, as_json, params=params, data=data)

    def _run_patch_request(self, url, params: dict = None, data: dict = None, as_json: bool = True):
        return self.__run_request(requests.patch, url, as_json, params=params, data=data)
