package com.pushtorefresh.storio.sqlite.integration;

import android.support.annotation.NonNull;

import com.pushtorefresh.storio.sqlite.BuildConfig;
import com.pushtorefresh.storio.sqlite.Changes;
import com.pushtorefresh.storio.sqlite.queries.RawQuery;
import com.pushtorefresh.storio.test.AbstractEmissionChecker;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.robolectric.RobolectricGradleTestRunner;
import org.robolectric.annotation.Config;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import rx.Subscription;
import rx.functions.Action1;

@RunWith(RobolectricGradleTestRunner.class)
@Config(constants = BuildConfig.class, sdk = 21)
public class ObserveChangesTest extends BaseTest {

    public class EmissionChecker extends AbstractEmissionChecker<Changes> {

        public EmissionChecker(@NonNull Queue<Changes> expected) {
            super(expected);
        }

        @Override
        @NonNull
        public Subscription subscribe() {
            return storIOSQLite
                    .observeChangesInTable(UserTableMeta.TABLE)
                    .subscribe(new Action1<Changes>() {
                        @Override
                        public void call(Changes changes) {
                            onNextObtained(changes);
                        }
                    });
        }
    }

    @Test
    public void insertEmission() {
        final List<User> users = TestFactory.newUsers(10);

        final Queue<Changes> expectedChanges = new LinkedList<Changes>();
        expectedChanges.add(Changes.newInstance(UserTableMeta.TABLE));

        final EmissionChecker emissionChecker = new EmissionChecker(expectedChanges);
        final Subscription subscription = emissionChecker.subscribe();

        putUsersBlocking(users);

        // Should receive changes of Users table
        emissionChecker.awaitNextExpectedValue();

        emissionChecker.assertThatNoExpectedValuesLeft();

        subscription.unsubscribe();
    }

    @Test
    public void updateEmission() {
        final List<User> users = putUsersBlocking(10);
        final List<User> updated = new ArrayList<User>(users.size());

        for (User user : users) {
            updated.add(User.newInstance(user.id(), user.email()));
        }

        final Queue<Changes> expectedChanges = new LinkedList<Changes>();
        expectedChanges.add(Changes.newInstance(UserTableMeta.TABLE));

        final EmissionChecker emissionChecker = new EmissionChecker(expectedChanges);
        final Subscription subscription = emissionChecker.subscribe();

        storIOSQLite
                .put()
                .objects(updated)
                .prepare()
                .executeAsBlocking();

        // Should receive changes of Users table
        emissionChecker.awaitNextExpectedValue();

        emissionChecker.assertThatNoExpectedValuesLeft();

        subscription.unsubscribe();
    }

    @Test
    public void deleteEmission() {
        final List<User> users = putUsersBlocking(10);

        final Queue<Changes> expectedChanges = new LinkedList<Changes>();
        expectedChanges.add(Changes.newInstance(UserTableMeta.TABLE));

        final EmissionChecker emissionChecker = new EmissionChecker(expectedChanges);
        final Subscription subscription = emissionChecker.subscribe();

        deleteUsersBlocking(users);

        // Should receive changes of Users table
        emissionChecker.awaitNextExpectedValue();

        emissionChecker.assertThatNoExpectedValuesLeft();

        subscription.unsubscribe();
    }

    @Test
    public void dummyTableEmission() {
        final String dummyTable = "dummy_table";    // There is no existing table in db
        final Queue<Changes> expectedChanges = new LinkedList<Changes>();
        expectedChanges.add(Changes.newInstance(dummyTable));

        final AbstractEmissionChecker emissionChecker = new AbstractEmissionChecker<Changes>(expectedChanges) {
            @NonNull
            @Override
            public Subscription subscribe() {
                return storIOSQLite
                        .observeChangesInTable(dummyTable)
                        .subscribe(new Action1<Changes>() {
                            @Override
                            public void call(Changes changes) {
                                onNextObtained(changes);
                            }
                        });
            }
        };

        final Subscription subscription = emissionChecker.subscribe();

        storIOSQLite
                .executeSQL()
                .withQuery(RawQuery.builder()
                        .query("delete from " + UserTableMeta.TABLE)
                        .affectsTables(dummyTable)  // Notify about dummy table changes
                        .build())
                .prepare()
                .executeAsBlocking();

        // Should receive changes of dummy table
        emissionChecker.awaitNextExpectedValue();

        emissionChecker.assertThatNoExpectedValuesLeft();

        subscription.unsubscribe();
    }
}
