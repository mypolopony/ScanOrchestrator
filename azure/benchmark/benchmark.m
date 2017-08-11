passes = 10;

% Sequential Blackjack
% [https://www.mathworks.com/help/distcomp/examples/sequential-blackjack.htm]

fprintf('\nSequential Blackjack --\n')
difficulty = pctdemo_helper_getDefaults();

sb = [];

for i = 1:passes
    [fig, numHands, numPlayers] = pctdemo_setup_blackjack(difficulty);
    startTime = clock;
    S = zeros(numHands, numPlayers); % Preallocate for the results.
    for i = 1:numPlayers
        S(:, i) = pctdemo_task_blackjack(numHands, 1);
    end
    elapsedTime = etime(clock, startTime);
    fprintf('Elapsed time is %2.1f seconds\n', elapsedTime);
    sb = [sb; elapsedTime];
end

fprintf('Average time is %2.1f seconds\n\n', mean(sb))


% Distributed Blackjack
% (uses parallel computing toolbox)
% [https://www.mathworks.com/help/distcomp/examples/distributed-blackjack.html]

fprintf('\n\nDistributed Blackjack --\n')

[difficulty, myCluster, numTasks] = pctdemo_helper_getDefaults();

db = [];

for i = 1:passes
    [fig, numHands, numPlayers] = pctdemo_setup_blackjack(difficulty);
    [splitPlayers, numTasks] = pctdemo_helper_split_scalar(numPlayers, ...
                                                      numTasks);
    fprintf(['This example will submit a job with %d task(s) ' ...
             'to the cluster.\n'], numTasks);
    startTime = clock;
    job = createJob(myCluster);
    for i = 1:numTasks
        createTask(job, @pctdemo_task_blackjack, 1, ...
                   {numHands, splitPlayers(i)});
    end
    submit(job);
    wait(job);
    try
        jobResults = fetchOutputs(job);
    catch err
        delete(job);
        rethrow(err);
    end
    S = cell2mat(jobResults');
    delete(job);
    elapsedTime = etime(clock, startTime);
    fprintf('Elapsed time is %2.1f seconds\n', elapsedTime);

    db = [db; elapsedTime];
end

fprintf('Average time is %2.1f seconds\n\n', mean(db))