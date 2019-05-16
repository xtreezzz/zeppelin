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

angular.module('zeppelinWebApp').directive('resizable', ResizableDirective);

function ResizableDirective() {
  let resizableConfig = {
    autoHide: true,
    handles: 'se',
    helper: 'resizable-helper',
    stop: function() {
      angular.element(this).css({'width': '100%', 'height': '100%'});
    },
  };

  let addEvent = function(config) {
    let removeEventByID = function(id) {
      let events = jQuery._data(config.element, 'events')[config.eventType];
      for (let i=0; i < events.length; i++) {
        if (events[i].data && events[i].data.eventID === id) {
          events.splice(i, 1);
          i--;
        }
      }
    };
    removeEventByID(config.eventID);
    angular.element(config.element).bind(config.eventType, {eventID: config.eventID}, config.handler);
    angular.element(config.onDestroyElement).scope().$on('$destroy', () => {
      removeEventByID(config.eventID);
    });
  };

  return {
    restrict: 'A',
    scope: {
      callback: '&onResize',
    },
    link: function postLink(scope, elem, attrs) {
      attrs.$observe('resize', function(resize) {
        let resetResize = function(elem, resize) {
          let colStep = window.innerWidth / 12;
          elem.off('resizestop');
          let conf = angular.copy(resizableConfig);
          if (resize.graphType === 'TABLE'
            || resize.graphType === 'NETWORK'
            || resize.graphType === 'TEXT'
            || resize.graphType === 'TEXT_APPEND') {
            conf.grid = [colStep, 10];
            conf.minHeight = 100;
          } else {
            conf.grid = [colStep, 10000];
            conf.minHeight = 0;
          }
          conf.maxWidth = window.innerWidth;

          elem.resizable(conf);
          elem.on('resizestop', function() {
            if (scope.callback) {
              let height = elem.height();
              if (height < 50) {
                height = 300;
              }
              scope.callback({width: Math.ceil(elem.width() / colStep), height: height});
            }
          });
        };

        resize = JSON.parse(resize);
        if (resize.allowresize === 'true') {
          resetResize(elem, resize);

          addEvent({
            eventID: elem[0].id,
            eventType: 'resize',
            element: window,
            onDestroyElement: elem[0],
            handler: () => resetResize(elem, resize),
          });
        }
      });
    },
  };
}
