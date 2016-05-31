/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.LambdaHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.Scoping;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.DedupGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.WherePredicateStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.PrunePathStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Ted Wilmes (http://twilmes.org)
 */
public final class PrunePathStrategy extends AbstractTraversalStrategy<TraversalStrategy.OptimizationStrategy> implements TraversalStrategy.OptimizationStrategy {

    private static final PrunePathStrategy INSTANCE = new PrunePathStrategy();

//    private static final Set<Class<? extends OptimizationStrategy>> PRIORS = new HashSet<>(Arrays.asList(PathProcessorStrategy.class));

    private PrunePathStrategy() {
    }

    public static PrunePathStrategy instance() {
        return INSTANCE;
    }

//    @Override
//    public Set<Class<? extends OptimizationStrategy>> applyPrior() {
//        return Collections.EMPTY_SET;
//    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        final Map<String, Integer> labelRefCount = new HashMap<>();
//        final Map<Step, List<String>> labelMaxStepMap = new HashMap<>();
        final Map<String, Integer> labelMaxStepMap = new HashMap<>();
        // gather labels and there max step mention
        // note max lamba step.  If one exists, nothing can be pruned until after that point
        // because we cannot introspect into the lambda and see if there was a label reference.
        Step lastLambdaStep = null;
        int stepCount = 0;
        for (final Step<?, ?> step : traversal.asAdmin().getSteps()) {
            if(step instanceof LambdaHolder) {
                lastLambdaStep = step;
            }
//            labelMaxStepMap.put(step, new ArrayList<>());
            final Set<String> labels = step.getLabels();
            for(final String label : labels) {
                if(labelRefCount.get(label) == null) {
                    labelRefCount.put(label, 1);
                } else {
                    labelRefCount.put(label, labelRefCount.get(label) + 1);
                }
            }
            if (step instanceof Scoping) {
                final Set<String> scopeKeys = ((Scoping)step).getScopeKeys();
                for(final String key : scopeKeys) {
                    if(labelRefCount.get(key) != null) {
                        labelRefCount.put(key, labelRefCount.get(key) - 1);
                    }
                }
//                labelMaxStepMap.get(step).addAll(scopeKeys);
                final int sCount = stepCount;
                scopeKeys.stream().forEach(key -> labelMaxStepMap.put(key, sCount));
                stepCount++;
            }
        }

        // do another pass and remove all unreferenced labels
        for (final Step<?, ?> step : traversal.asAdmin().getSteps()) {
            for (final String label : step.getLabels()) {
                if(labelRefCount.get(label) > 0) {
                    System.out.println("Removing label " + label);
                    step.removeLabel(label);
                }
            }
        }

        // go through backwards
        Step currentStep = traversal.asAdmin().getEndStep();
        Set<String> foundLabels = new HashSet<>();
        try {
            while (true) {
                Set<String> labels = currentStep.getLabels();
                Set<String> dropThese = new HashSet<>();
                for(final String label : labels) {
                    if(!foundLabels.contains(label)) {
                        dropThese.add(label);
                        foundLabels.add(label);
                    }
                }
                if(!dropThese.isEmpty()) {
                    TraversalHelper.insertAfterStep(
                            new PrunePathStep<>(traversal, true, dropThese.toArray(new String[dropThese.size()])), currentStep, traversal.asAdmin());
                }
                currentStep = currentStep.getPreviousStep();
            }
        } catch (FastNoSuchElementException e) {

        }



        // insert prunePathStep after max step
//        for (final Map.Entry<String, Integer> entry : labelMaxStepMap.entrySet()) {
////            List<String> labels = entry.getValue();
////            if (!labels.isEmpty()) {
//                Step step = entry.getKey();
//                if(! (step.getNextStep() instanceof EmptyStep)) {
//                    // if there aren't any labels to drop after this, go ahead and
//                    // drop the full path and skip the labels
//                    boolean dropPath = true;
//                    TraversalHelper.insertAfterStep(new PrunePathStep<>(traversal, dropPath, labels.toArray(new String[labels.size()])), step, traversal.asAdmin());
//                }
//            }
//        }
    }
}
