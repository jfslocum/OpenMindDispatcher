#!/cm/shared/openmind/anaconda/1.9.2/bin/python
import sys

neuronTypes = ['dormant, unconnected', 'dormant, excited', 
               'moderate, inhibited', 'moderate, unconnected', 
               'moderate, excited', 'active, inhibited', 
               'active, unconnected']

noise_coeffs = [5, 10, 20, 30]


OUTPUT_FOLDER = "/om/user/jslocum/hmm2dlearning/hmmtrainresults"
MATLAB_CALL = """matlab -singleCompThread -nojvm -nodisplay -nosplash -r "addpath ../; addpath ../../; addpath ../helpers; runTrials('%s', '%s', '%s', '%s', 100, %i, 'modelChange', 800, '%s')"\n"""

def writeWorklistToFile(f):
    for truth_idx in range(pow(len(neuronTypes), 2)):
        true_type1 = neuronTypes[truth_idx % len(neuronTypes)]
        true_type2 = neuronTypes[truth_idx / len(neuronTypes)]
        for guess_idx in range(pow(len(neuronTypes), 2)):
            guess_type1 = neuronTypes[guess_idx % len(neuronTypes)]
            guess_type2 = neuronTypes[guess_idx / len(neuronTypes)]
            for n in range(len(noise_coeffs)):
                noise = noise_coeffs[n]
                f.write(MATLAB_CALL % (true_type1, true_type2,
                                       guess_type1, guess_type2, noise, OUTPUT_FOLDER))


with open(sys.argv[1], 'w') as f:
    writeWorklistToFile(f)
