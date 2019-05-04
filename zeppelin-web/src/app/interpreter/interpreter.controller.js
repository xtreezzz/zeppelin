/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

angular.module('zeppelinWebApp').controller('InterpreterCtrl', InterpreterCtrl);

function InterpreterCtrl($rootScope, $scope, $http, baseUrlSrv, websocketMsgSrv, ngToast, $timeout, $route) {
  'ngInject';

  $scope.moduleTypes = ['INTERPRETER', 'COMPLETER'];
  $scope.modules = [];
  $scope.creatingModuleSource = {};
  $scope.creatingModuleConfiguration = {};
  $scope.creatingModuleConfigurationMode = 'none';

  $scope.searchInterpreter = '';
  $scope._ = _;
  ngToast.dismiss();

  // Get configuration for interpreter owners selectors.
  // see https://select2.org/configuration/options-api
  let getOwnersSelectConfiguration = function() {
    let selectJson = {
      tags: true,
      minimumInputLength: 3,
      multiple: true,
      tokenSeparators: [',', ' '],
      ajax: {
        url: function(params) {
          if (!params.term) {
            return false;
          }
          return baseUrlSrv.getRestApiBase() + '/security/userlist/' + params.term;
        },
        delay: 250,
        processResults: function(data, params) {
          let results = [];

          if (data.body.users.length !== 0) {
            let users = [];
            for (let len = 0; len < data.body.users.length; len++) {
              users.push({
                'id': data.body.users[len].trim(),
                'text': data.body.users[len].trim(),
              });
            }
            results.push({
              'text': 'Users :',
              'children': users,
            });
          }
          if (data.body.roles.length !== 0) {
            let roles = [];
            for (let len = 0; len < data.body.roles.length; len++) {
              roles.push({
                'id': data.body.roles[len].trim(),
                'text': data.body.roles[len].trim(),
              });
            }
            results.push({
              'text': 'Roles :',
              'children': roles,
            });
          }
          return {
            results: results,
            pagination: {
              more: false,
            },
          };
        },
        cache: false,
      },
    };
    return selectJson;
  };

  $scope.togglePermissions = function(interpreterShebang) {
    // re-initialize given selector
    angular.element('#' + interpreterShebang + 'Owners').select2(getOwnersSelectConfiguration());
  };

  $scope.$on('ngRenderFinished', function(event, data) {
    // re-initialize all selectors
    console.error('ngRenderFinished');
    for (let moduleIdx = 0; moduleIdx < $scope.modules.length; moduleIdx++) {
      if ($scope.modules[moduleIdx].modules) {
        for (let configurationIdx = 0; configurationIdx < $scope.modules[moduleIdx].modules.length;
             configurationIdx++) {
          let shebang = $scope.modules[moduleIdx].modules[configurationIdx].configuration.shebang;
          angular.element('#' + shebang + 'Owners').select2(getOwnersSelectConfiguration());
        }
      }
    }
    angular.element('#newInterpreterOwners').select2(getOwnersSelectConfiguration());
  });

  let getModuleSources = function() {
    $http.get(baseUrlSrv.getRestApiBase() + '/modules/list').then(function(res) {
      $scope.modules = res.data.body.modules;
      console.log('getModuleSources', $scope.modules);
    }).catch(function(res) {
      $scope.showErrorMessage(res);
    });
  };


  $scope.clearCreatingModuleSource = function() {
    $scope.creatingModuleSource.name = '';
    $scope.creatingModuleSource.artifact = '';
    $scope.creatingModuleSource.type = '';
  };

  $scope.addNewModuleSource = function() {
    console.log('$scope.creatingModuleSource ', $scope.creatingModuleSource);

    $http({
      method: 'POST',
      url: baseUrlSrv.getRestApiBase() + '/modules/addModuleSource',
      headers: {
        'Content-Type': 'text/plain',
      },
      data: $scope.creatingModuleSource,
    })
      .then(function(res) {
        getModuleSources();
        angular.element('#srcModal').modal('hide');
      }).catch(function(res) {
        $scope.showErrorMessage(res);
      });
  };

  $scope.setReinstallOnStartModuleSource = function(moduleSourceId, reinstall) {
    let request = {};
    request.id = moduleSourceId;
    request.reinstall = reinstall;
    console.log('setReinstallOnStartModuleSource : ', request);

    $http({
      method: 'POST',
      url: baseUrlSrv.getRestApiBase() + '/modules/setReinstallOnStartModuleSource',
      headers: {
        'Content-Type': 'text/plain',
      },
      data: request,
    })
      .then(function(res) {
        getModuleSources();
      }).catch(function(res) {
        $scope.showErrorMessage(res);
      });
  };

  $scope.installModuleSource = function(moduleSourceId) {
    let request = {};
    request.id = moduleSourceId;
    console.log('installModuleSource : ', request);

    $http({
      method: 'POST',
      url: baseUrlSrv.getRestApiBase() + '/modules/installModuleSource',
      headers: {
        'Content-Type': 'text/plain',
      },
      data: request,
    })
      .then(function(res) {
        getModuleSources();
      }).catch(function(res) {
        $scope.showErrorMessage(res);
      });
  };

  $scope.uninstallModuleSource = function(moduleSourceId) {
    let request = {};
    request.id = moduleSourceId;
    console.log('uninstallModuleSource : ', request);

    $http({
      method: 'POST',
      url: baseUrlSrv.getRestApiBase() + '/modules/uninstallModuleSource',
      headers: {
        'Content-Type': 'text/plain',
      },
      data: request,
    })
      .then(function(res) {
        getModuleSources();
      }).catch(function(res) {
        $scope.showErrorMessage(res);
      });
  };

  $scope.deleteModuleSource = function(moduleSourceId) {
    let request = {};
    request.id = moduleSourceId;
    console.log('deleteModuleSource : ', request);

    $http({
      method: 'POST',
      url: baseUrlSrv.getRestApiBase() + '/modules/deleteModuleSource',
      headers: {
        'Content-Type': 'text/plain',
      },
      data: request,
    })
      .then(function(res) {
        getModuleSources();
      }).catch(function(res) {
        $scope.showErrorMessage(res);
      });
  };

  $scope.setSelectedModuleConfiguration = function(moduleSource, moduleConfiguration, innerConfiguration, mode) {
    console.log('moduleSource ', moduleSource);
    console.log('moduleConfiguration ', moduleConfiguration);
    console.log('innerConfiguration ', innerConfiguration);

    $scope.creatingModuleConfiguration.moduleSource = angular.copy(moduleSource);
    $scope.creatingModuleConfiguration.innerConfiguration = angular.copy(innerConfiguration);

    if(moduleConfiguration === null) {
      $scope.creatingModuleConfiguration.moduleConfiguration = {};
      $scope.creatingModuleConfiguration.moduleConfiguration.shebang = '';
      $scope.creatingModuleConfiguration.moduleConfiguration.humanReadableName = '';
      $scope.creatingModuleConfiguration.moduleConfiguration.jvmOptions = '';
      $scope.creatingModuleConfiguration.moduleConfiguration.concurrentTasks = 10;
      $scope.creatingModuleConfiguration.moduleConfiguration.moduleInnerConfigId = -1;
      $scope.creatingModuleConfiguration.moduleConfiguration.moduleSourceId = moduleSource.id;
      $scope.creatingModuleConfiguration.moduleConfiguration.permissions = {};
      $scope.creatingModuleConfiguration.moduleConfiguration.permissions.isEnabled = false;
      $scope.creatingModuleConfiguration.moduleConfiguration.permissions.owners = [];
      $scope.creatingModuleConfiguration.moduleConfiguration.isEnabled = false;
    } else {
      $scope.creatingModuleConfiguration.moduleConfiguration = angular.copy(moduleConfiguration);
    }

    $scope.creatingModuleConfigurationMode = mode;
  };

  $scope.addCustomModuleConfiguration = function() {
    $scope.creatingModuleConfiguration.moduleConfiguration.permissions.owners =
    angular.element('#newInterpreterOwners').val();

    $http({
      method: 'POST',
      url: baseUrlSrv.getRestApiBase() + '/modules/addModuleConfiguration',
      headers: {
        'Content-Type': 'text/plain',
      },
      data: $scope.creatingModuleConfiguration,
    })
      .then(function(res) {
        getModuleSources();
        angular.element('#interpreterModal').modal('hide');
      }).catch(function(res) {
        $scope.showErrorMessage(res);
      });
  };

  $scope.updateCustomModuleConfiguration = function() {
    let owners = angular.element('#newInterpreterOwners').val();
    for (let i = 0; i < owners.length; i++) {
      console.error(owners[i] === owners[i].trim());
      owners[i] = owners[i].trim();
      console.error(owners[i] === owners[i].trim());
    }
    $scope.creatingModuleConfiguration.moduleConfiguration.permissions.owners = angular.copy(owners);

    $http({
      method: 'POST',
      url: baseUrlSrv.getRestApiBase() + '/modules/updateModuleConfiguration',
      headers: {
        'Content-Type': 'text/plain',
      },
      data: $scope.creatingModuleConfiguration,
    })
      .then(function(res) {
        getModuleSources();
        angular.element('#interpreterModal').modal('hide');
      }).catch(function(res) {
        $scope.showErrorMessage(res);
      });
  };


  $scope.enableCustomModule = function(moduleSourceId, enabled) {
    let request = {};
    request.id = moduleSourceId;
    request.enable = enabled;
    console.log('deleteModuleSource : ', request);

    $http({
      method: 'POST',
      url: baseUrlSrv.getRestApiBase() + '/modules/enableModule',
      headers: {
        'Content-Type': 'text/plain',
      },
      data: request,
    })
      .then(function(res) {
        getModuleSources();
      }).catch(function(res) {
        $scope.showErrorMessage(res);
      });
  };

  $scope.restartCustomModule = function(moduleSourceId) {
    let request = {};
    request.id = moduleSourceId;
    console.log('deleteModuleSource : ', request);

    $http({
      method: 'POST',
      url: baseUrlSrv.getRestApiBase() + '/modules/restartModule',
      headers: {
        'Content-Type': 'text/plain',
      },
      data: request,
    })
      .then(function(res) {
        getModuleSources();
      }).catch(function(res) {
        $scope.showErrorMessage(res);
      });
  };

  $scope.removeCustomModule = function(moduleSourceId) {
    let request = {};
    request.id = moduleSourceId;
    console.log('deleteModuleSource : ', request);

    $http({
      method: 'POST',
      url: baseUrlSrv.getRestApiBase() + '/modules/deleteModule',
      headers: {
        'Content-Type': 'text/plain',
      },
      data: request,
    })
      .then(function(res) {
        getModuleSources();
      }).catch(function(res) {
        $scope.showErrorMessage(res);
      });
  };
  // common

  $scope.showErrorMessage = function(res) {
    BootstrapDialog.show({
      closable: false,
      closeByBackdrop: false,
      closeByKeyboard: false,
      title: 'Error: ' + res.status,
      message: _.escape(res.data ? res.data.message : ''),
      buttons: [{
        // close all the dialogs when there are error on running all paragraphs
        label: 'Close',
        action: function() {
          BootstrapDialog.closeAll();
        },
      }],
    });
  };

  let init = function() {
    getModuleSources();
  };

  $scope.getInterpreterBindingModeDocsLink = function() {
    const currentVersion = $rootScope.zeppelinVersion;
    return `https://zeppelin.apache.org/docs/${currentVersion}/usage/interpreter/interpreter_binding_mode.html`;
  };

  init();
}
