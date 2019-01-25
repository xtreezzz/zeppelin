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

angular.module('zeppelinWebApp').service('favoriteNotesService', FavoriteNotesService);

function FavoriteNotesService($rootScope, $http, baseUrlSrv) {
  let favoriteNoteIds = new Set();
  let recentNoteIds = new Set();
  let username;
  let loadPromise;

  this.init = function() {
    if (!$rootScope.ticket || !$rootScope.ticket.screenUsername) {
      return;
    }
    username = $rootScope.ticket.screenUsername;
    loadPromise = new Promise(function(resolve, reject) {
      $http.get(
        `${baseUrlSrv.getRestApiBase()}/favorite_notes/get_notes_ids?username=`
        + username)
      .then(function(response) {
        if (response.data.status === 'OK') {
          favoriteNoteIds = new Set(response.data.body.favorite);
          recentNoteIds = new Set(response.data.body.recent);
          resolve();
        }
      });
    });
  };

  this.noteIsFavorite = function(noteId) {
    return favoriteNoteIds.has(noteId);
  };

  this.noteIsRecent = function(noteId) {
    return recentNoteIds.has(noteId);
  };

  this.filterFavoriteNotes = function(allNotes, thenAction) {
    if (loadPromise) {
      loadPromise.then(() => {
        let onlyFavorite = allNotes.filter((n) => this.noteIsFavorite(n.id));
        generaNameFromPath(onlyFavorite);
        thenAction(onlyFavorite);
      });
    }
  };

  this.filterRecentNotes = function(allNotes, thenAction) {
    if (loadPromise) {
      loadPromise.then(() => {
        let onlyRecent = [];
        [...recentNoteIds].reverse().forEach((id) => {
          for (let i=0; i < allNotes.length; i++) {
            if (allNotes[i].id === id) {
              onlyRecent.push(allNotes[i]);
              break;
            }
          }
        });
        generaNameFromPath(onlyRecent);
        thenAction(onlyRecent);
      });
    }
  };

  function generaNameFromPath(notes) {
    notes.forEach((note) => {
      note.name = note.path.substring(note.path.lastIndexOf('/') + 1);
    });
  }

  let changeNoteStatus = function(conf) {
    if (!username) {
      return;
    }
    let {
      noteId,
      noteType,
      noteAction,
      thenAction,
    } = conf;

    return $http.get(`${baseUrlSrv.getRestApiBase()}/favorite_notes/set_note_status` +
      '?username=' + username +
      '&note_id=' + noteId +
      '&note_type=' + noteType +
      '&note_action=' + noteAction).then((response) => {
        if (response.data.status === 'OK' && thenAction) {
          thenAction();
        }
      });
  };

  this.addNoteToFavorite = function(noteId) {
    changeNoteStatus({
      noteId: noteId,
      noteType: 'favorite',
      noteAction: 'add',
      thenAction: () => favoriteNoteIds.add(noteId),
    });
  };

  this.removeNoteFromFavorite = function(noteId) {
    changeNoteStatus({
      noteId: noteId,
      noteType: 'favorite',
      noteAction: 'remove',
      thenAction: () => favoriteNoteIds.delete(noteId),
    });
  };

  this.addNoteToRecent = function(noteId) {
    changeNoteStatus({
      noteId: noteId,
      noteType: 'recent',
      noteAction: 'add',
      thenAction: () => recentNoteIds.add(noteId),
    });
  };

  this.removeNoteFromResent = function(noteId) {
    changeNoteStatus({
      noteId: noteId,
      noteType: 'recent',
      noteAction: 'remove',
      thenAction: () => recentNoteIds.delete(noteId),
    });
  };
}
